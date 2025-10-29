# Implementation Sequence Diagram: Parquet-Based Checkpointing ETL

## Overview

This document provides detailed sequence diagrams for the proposed parquet-based checkpointing solution for processing large .dat files in the ZTDWR transport intelligence pipeline.

## Diagram 1: Fresh Start (No Existing Checkpoint)

This shows the complete flow when processing a file for the first time.

```mermaid
sequenceDiagram
    participant Blob as Azure Blob Storage
    participant Func as Azure Function
    participant Parser as Streaming Parser
    participant CheckMgr as Checkpoint Manager
    participant Storage as Checkpoint Storage
    participant Validator as Data Validator
    participant Transformer as Data Transformer
    participant DB as PostgreSQL

    Note over Blob,DB: PHASE 1: Convert .dat to Parquet Chunks

    Blob->>Func: Blob trigger: ZTDWR_20250129.dat (100MB)
    Func->>Func: Generate sync_id
    Func->>CheckMgr: Initialize(sync_id, file_name)
    CheckMgr->>Storage: Check if checkpoint exists
    Storage-->>CheckMgr: Not found (fresh start)

    Func->>Parser: parse_ztdwr_chunks(raw_content, chunk_size=10000)
    Parser->>Parser: Decompress if gzipped
    Parser->>Parser: Detect delimiter (~^)
    Parser->>Parser: Normalize headers

    loop For each chunk (0 to N)
        Parser-->>Func: Yield chunk DataFrame (10K rows)
        Func->>CheckMgr: save_chunk_parquet(chunk_id, chunk_df)
        CheckMgr->>Storage: Write chunk_0000.parquet
        Storage-->>CheckMgr: Success (500KB compressed)
        CheckMgr->>CheckMgr: Update manifest.json
        CheckMgr-->>Func: Chunk saved
    end

    Func->>CheckMgr: finalize_conversion()
    CheckMgr->>Storage: Update manifest (status: ready)

    Note over Blob,DB: PHASE 2: Process Parquet Chunks

    Func->>CheckMgr: get_pending_chunks()
    CheckMgr-->>Func: [0, 1, 2, ..., N] (all chunks pending)

    loop For each chunk_id in pending
        Func->>CheckMgr: load_chunk(chunk_id)
        CheckMgr->>Storage: Read chunk_0000.parquet
        Storage-->>CheckMgr: DataFrame (10K rows)
        CheckMgr-->>Func: chunk_df

        Func->>Validator: validate_ztdwr_data(chunk_df)
        Validator->>Validator: Check required columns
        Validator->>Validator: Validate data types
        Validator->>Validator: Check business rules
        Validator-->>Func: validation_errors (if any)

        alt Has validation errors
            Func->>Func: Remove invalid rows
            Func->>Func: Track validation errors
        end

        Func->>Transformer: transform_to_transport_documents(chunk_df)
        Transformer->>Transformer: Map columns
        Transformer->>Transformer: Convert data types
        Transformer->>Transformer: Combine date+time fields
        Transformer-->>Func: transformed_df

        Func->>DB: upsert_to_postgres(transformed_df)
        DB->>DB: BEGIN transaction
        DB->>DB: INSERT ... ON CONFLICT DO UPDATE
        DB->>DB: COMMIT transaction
        DB-->>Func: (inserted=9850, updated=150)

        Func->>CheckMgr: mark_chunk_complete(chunk_id, inserted, updated)
        CheckMgr->>Storage: Update manifest.json
        Storage-->>CheckMgr: Success
    end

    Func->>DB: refresh_materialized_views()
    DB-->>Func: Success

    Func->>CheckMgr: cleanup()
    CheckMgr->>Storage: Delete checkpoint directory
    Storage-->>CheckMgr: Deleted

    Func->>DB: update_sync_metadata(status=SUCCESS)
    DB-->>Func: Success

    Func-->>Blob: Sync complete
```

## Diagram 2: Resume from Failure

This shows the flow when resuming a previously failed sync.

```mermaid
sequenceDiagram
    participant Blob as Azure Blob Storage
    participant Func as Azure Function (Retry)
    participant CheckMgr as Checkpoint Manager
    participant Storage as Checkpoint Storage
    participant Validator as Data Validator
    participant Transformer as Data Transformer
    participant DB as PostgreSQL

    Note over Blob,DB: RETRY SCENARIO: Resume from chunk 120

    Blob->>Func: Manual retry or automatic retry
    Func->>Func: Use existing sync_id (from retry context)
    Func->>CheckMgr: Initialize(sync_id, file_name)
    CheckMgr->>Storage: Check if checkpoint exists
    Storage-->>CheckMgr: Found! (manifest.json)

    CheckMgr->>Storage: Load manifest.json
    Storage-->>CheckMgr: Manifest loaded
    CheckMgr->>CheckMgr: Parse manifest

    Note over CheckMgr: Manifest shows:<br/>- Total chunks: 150<br/>- Completed: 0-119<br/>- Failed: 120<br/>- Pending: 121-150

    Func->>CheckMgr: get_pending_chunks()
    CheckMgr-->>Func: [120, 121, 122, ..., 150] (31 chunks)

    Note over Func: ✅ Skip Phase 1 (parquet already exists)<br/>✅ Skip chunks 0-119 (already completed)

    loop For chunk_id in [120, 121, ..., 150]
        Func->>CheckMgr: load_chunk(chunk_id)
        CheckMgr->>Storage: Read chunk_0120.parquet
        Storage-->>CheckMgr: DataFrame (10K rows)
        CheckMgr-->>Func: chunk_df (loaded in 0.2s vs 10s from .dat)

        Func->>Validator: validate_ztdwr_data(chunk_df)
        Validator-->>Func: validation_errors

        Func->>Transformer: transform_to_transport_documents(chunk_df)
        Transformer-->>Func: transformed_df

        Func->>DB: upsert_to_postgres(transformed_df)

        alt Database Success
            DB-->>Func: (inserted=9800, updated=200)
            Func->>CheckMgr: mark_chunk_complete(chunk_id, 9800, 200)
            CheckMgr->>Storage: Update manifest: chunk 120 = completed
        else Database Error (Transient)
            DB-->>Func: Error: Connection timeout
            Func->>CheckMgr: mark_chunk_failed(chunk_id, error_msg)
            CheckMgr->>Storage: Update manifest: chunk 120 = failed
            Note over Func: Continue to next chunk<br/>(will retry chunk 120 on next run)
        end
    end

    Func->>CheckMgr: get_pending_chunks()
    CheckMgr-->>Func: [] (all complete)

    Func->>DB: refresh_materialized_views()
    Func->>CheckMgr: cleanup()
    CheckMgr->>Storage: Delete checkpoint directory
    Func->>DB: update_sync_metadata(status=SUCCESS)
    Func-->>Blob: Sync complete (resumed successfully)
```

## Diagram 3: Checkpoint Manager Internal Flow

This shows the detailed operations within the Checkpoint Manager.

```mermaid
sequenceDiagram
    participant Caller as Azure Function
    participant CM as CheckpointManager
    participant FS as File System / Blob Storage
    participant Manifest as manifest.json

    Note over Caller,Manifest: Initialize Checkpoint

    Caller->>CM: new CheckpointManager(sync_id, file_name, base_path)
    CM->>CM: checkpoint_dir = base_path/sync_id
    CM->>CM: manifest_path = checkpoint_dir/manifest.json

    Caller->>CM: checkpoint_exists()
    CM->>FS: Check if manifest.json exists
    alt Manifest exists
        FS-->>CM: True
        CM->>FS: Read manifest.json
        FS-->>CM: JSON content
        CM->>CM: Parse and validate manifest
        CM-->>Caller: True (checkpoint exists)
    else Manifest not found
        FS-->>CM: False
        CM-->>Caller: False (fresh start)
    end

    Note over Caller,Manifest: Create New Checkpoint

    Caller->>CM: create_checkpoint(total_chunks, chunk_size, file_size)
    CM->>CM: Build manifest structure

    rect rgb(200, 220, 250)
        Note over CM,Manifest: Manifest Structure
        CM->>CM: {<br/>  sync_id: "...",<br/>  total_chunks: 150,<br/>  chunk_size: 10000,<br/>  status: "converting",<br/>  chunks: []<br/>}
    end

    CM->>FS: Write manifest.json
    FS-->>CM: Success

    Note over Caller,Manifest: Save Chunk as Parquet

    loop For each chunk
        Caller->>CM: save_chunk_parquet(chunk_id, chunk_df)
        CM->>CM: parquet_path = f"chunk_{chunk_id:04d}.parquet"

        CM->>CM: chunk_df.to_parquet(<br/>  path,<br/>  engine='pyarrow',<br/>  compression='snappy'<br/>)

        CM->>FS: Write parquet file
        FS-->>CM: File written (500KB)

        CM->>FS: Get file size
        FS-->>CM: 524288 bytes

        CM->>CM: Build chunk metadata
        rect rgb(200, 250, 220)
            Note over CM: {<br/>  chunk_id: 0,<br/>  status: "ready",<br/>  rows: 10000,<br/>  parquet_path: "chunk_0000.parquet",<br/>  parquet_size_bytes: 524288,<br/>  created_at: "2025-01-29T14:32:15Z"<br/>}
        end

        CM->>CM: Append to manifest.chunks[]
        CM->>FS: Update manifest.json
        FS-->>CM: Success
        CM-->>Caller: Chunk saved
    end

    Note over Caller,Manifest: Load and Process Chunk

    Caller->>CM: get_pending_chunks()
    CM->>CM: Filter chunks where status in [ready, failed, pending]
    CM-->>Caller: [chunk_ids]

    loop For each chunk_id
        Caller->>CM: load_chunk(chunk_id)
        CM->>CM: parquet_path = f"chunk_{chunk_id:04d}.parquet"
        CM->>FS: Read parquet file
        FS-->>CM: Parquet binary data
        CM->>CM: pd.read_parquet(parquet_path)
        CM-->>Caller: DataFrame

        Note over Caller: Process chunk<br/>(validate, transform, upsert)

        alt Processing Success
            Caller->>CM: mark_chunk_complete(chunk_id, inserted, updated)
            CM->>CM: Find chunk in manifest.chunks
            CM->>CM: Update: status = "completed"<br/>processed_at = now()<br/>records_inserted = inserted<br/>records_updated = updated
            CM->>FS: Update manifest.json
            FS-->>CM: Success
            CM-->>Caller: Marked complete
        else Processing Failure
            Caller->>CM: mark_chunk_failed(chunk_id, error_msg)
            CM->>CM: Find chunk in manifest.chunks
            CM->>CM: Update: status = "failed"<br/>failed_at = now()<br/>error = error_msg
            CM->>FS: Update manifest.json
            FS-->>CM: Success
            CM-->>Caller: Marked failed
        end
    end

    Note over Caller,Manifest: Cleanup After Success

    Caller->>CM: cleanup()
    CM->>FS: Delete all chunk_*.parquet files
    FS-->>CM: Deleted
    CM->>FS: Delete manifest.json
    FS-->>CM: Deleted
    CM->>FS: Remove checkpoint directory
    FS-->>CM: Directory removed
    CM-->>Caller: Cleanup complete
```

## Diagram 4: Error Handling Scenarios

This shows how different types of errors are handled.

```mermaid
flowchart TD
    Start([Process Chunk N]) --> LoadParquet[Load chunk_N.parquet]
    LoadParquet --> Validate[Validate Data]

    Validate --> ValidCheck{Validation<br/>Errors?}
    ValidCheck -->|No errors| Transform[Transform Data]
    ValidCheck -->|Row-level errors| RemoveInvalid[Remove Invalid Rows]
    ValidCheck -->|Global errors<br/>Missing required column| FatalError[Mark Sync as FAILED]

    RemoveInvalid --> ErrorRate{Error Rate<br/>> Threshold?}
    ErrorRate -->|Yes > 10%| FatalError
    ErrorRate -->|No <= 10%| Transform

    Transform --> Upsert[Upsert to PostgreSQL]

    Upsert --> UpsertCheck{Upsert<br/>Success?}

    UpsertCheck -->|Success| MarkComplete[Mark chunk_N as COMPLETED]
    UpsertCheck -->|Transient Error<br/>Connection timeout| MarkFailed[Mark chunk_N as FAILED]
    UpsertCheck -->|Fatal Error<br/>Invalid data type| FatalError

    MarkComplete --> NextChunk{More<br/>Chunks?}
    MarkFailed --> NextChunk

    NextChunk -->|Yes| LoadParquet
    NextChunk -->|No| CheckFailed{Any Failed<br/>Chunks?}

    CheckFailed -->|Yes| PartialSuccess[Status: PARTIAL_SUCCESS<br/>Keep checkpoints for retry]
    CheckFailed -->|No| Success[Status: SUCCESS<br/>Cleanup checkpoints]

    FatalError --> Abort[Abort Sync<br/>Keep checkpoints<br/>Send alert email]

    PartialSuccess --> End([End - Retry Later])
    Success --> End
    Abort --> End

    style FatalError fill:#ffcccc
    style MarkFailed fill:#ffffcc
    style MarkComplete fill:#ccffcc
    style Success fill:#ccffcc
    style PartialSuccess fill:#ffffcc
```

## Diagram 5: Data Flow Architecture

This shows the overall system architecture with checkpointing.

```mermaid
graph TB
    subgraph "Azure Blob Storage"
        SourceBlob[Source .dat File<br/>ZTDWR_20250129.dat<br/>100MB]
        CheckpointDir[Checkpoint Directory<br/>checkpoints/sync_xyz/]
        Manifest[manifest.json<br/>Metadata & Status]
        Parquet1[chunk_0000.parquet<br/>500KB]
        Parquet2[chunk_0001.parquet<br/>500KB]
        ParquetN[chunk_0149.parquet<br/>500KB]
    end

    subgraph "Azure Function"
        Trigger[Blob Trigger<br/>ztdwr_sync]
        Pipeline[Streaming Pipeline<br/>with Checkpoints]
        CheckMgr[Checkpoint Manager]
    end

    subgraph "Processing Components"
        Parser[Streaming Parser<br/>Parse .dat to chunks]
        Validator[Data Validator<br/>Check quality]
        Transformer[Data Transformer<br/>Map to schema]
    end

    subgraph "Database"
        Postgres[(PostgreSQL<br/>transport_documents)]
        SyncMeta[(sync_metadata<br/>Status tracking)]
        MatView[Materialized Views]
    end

    subgraph "Monitoring"
        Logs[Azure Logs<br/>Application Insights]
        Email[Alert Emails<br/>On failure]
    end

    SourceBlob -->|Trigger| Trigger
    Trigger -->|1. Check for checkpoint| CheckMgr
    CheckMgr <-->|Read/Write| CheckpointDir
    CheckpointDir --> Manifest
    CheckpointDir --> Parquet1
    CheckpointDir --> Parquet2
    CheckpointDir --> ParquetN

    Pipeline -->|Phase 1: Convert| Parser
    Parser -->|Save chunks| CheckMgr
    CheckMgr -->|Write parquet| Parquet1
    CheckMgr -->|Write parquet| Parquet2
    CheckMgr -->|Write parquet| ParquetN

    Pipeline -->|Phase 2: Process| CheckMgr
    CheckMgr -->|Load chunks| Validator
    Validator --> Transformer
    Transformer -->|Upsert batches| Postgres

    Postgres -->|Refresh| MatView
    Pipeline -->|Track progress| SyncMeta
    Pipeline -->|Log events| Logs
    Pipeline -->|On error| Email

    Pipeline -.->|On success: cleanup| CheckpointDir

    style SourceBlob fill:#e1f5ff
    style CheckpointDir fill:#fff3cd
    style Postgres fill:#d4edda
    style Pipeline fill:#cce5ff
```

## Key Insights from Diagrams

### 1. Two-Phase Processing
- **Phase 1 (Convert):** One-time conversion of .dat to parquet chunks
- **Phase 2 (Process):** Resumable processing of parquet chunks

### 2. Fast Resume Path
- On retry: Skip Phase 1 entirely
- Load manifest to identify pending chunks
- Process only failed/pending chunks

### 3. Granular Error Handling
- **Transient errors:** Mark chunk as failed, retry later
- **Data quality errors:** Remove invalid rows, continue
- **Fatal errors:** Abort sync, alert team

### 4. Storage Efficiency
- Parquet compression: 10-20% of original size
- Cleanup on success: No wasted storage
- Retention policy: Auto-delete after 7 days

### 5. Observable Progress
- Manifest tracks per-chunk status
- Easy to see: "120/150 chunks completed"
- Metrics: Time saved, chunks skipped, etc.

## Implementation Notes

### Checkpoint Directory Structure

```
checkpoints/
  sync_20250129_143022_a3f4b2c1/
    manifest.json              # 15KB - Metadata
    chunk_0000.parquet         # 500KB - Chunk 0
    chunk_0001.parquet         # 500KB - Chunk 1
    chunk_0002.parquet         # 500KB - Chunk 2
    ...
    chunk_0149.parquet         # 500KB - Chunk 149
```

**Total size:** ~75MB for 150 chunks (vs 100MB original .dat)

### Manifest Schema

```json
{
  "sync_id": "sync_20250129_143022_a3f4b2c1",
  "source_file": "ZTDWR_20250129_LARGE.dat",
  "source_path": "data/hex-ztdwr/ZTDWR_20250129_LARGE.dat",
  "file_size_bytes": 104857600,
  "total_chunks": 150,
  "chunk_size": 10000,
  "status": "processing",
  "created_at": "2025-01-29T14:30:22Z",
  "updated_at": "2025-01-29T14:45:10Z",
  "phase": "processing",
  "chunks": [
    {
      "chunk_id": 0,
      "status": "completed",
      "rows": 10000,
      "parquet_path": "chunk_0000.parquet",
      "parquet_size_bytes": 524288,
      "created_at": "2025-01-29T14:31:00Z",
      "processed_at": "2025-01-29T14:32:15Z",
      "records_inserted": 9850,
      "records_updated": 150,
      "validation_errors": 0
    }
  ]
}
```

### Chunk Status States

```
ready     -> Parquet created, ready to process
processing -> Currently being processed
completed  -> Successfully processed and upserted
failed     -> Processing failed, will retry
skipped    -> Intentionally skipped (e.g., all invalid rows)
```

## Performance Estimates

### Scenario: 100MB file, 150,000 rows, 150 chunks

| Metric | Without Checkpointing | With Checkpointing | Improvement |
|--------|----------------------|--------------------|-------------|
| **First Run** | 7 min | 8 min | -1 min (overhead) |
| **Retry after 80% complete** | 7 min (restart) | 2 min (resume) | **5 min saved (71%)** |
| **Retry after 50% complete** | 7 min (restart) | 4 min (resume) | **3 min saved (43%)** |
| **Memory usage** | 800MB | 600MB | **25% reduction** |
| **Storage cost/day** | $0 | $0.0003 | Negligible |

### Break-Even Point

- **Overhead:** +1 minute on fresh start (parquet conversion)
- **Benefit:** 5-7 minutes saved on retry
- **Break-even:** If >15% of syncs require retry, checkpointing is net positive

## Next Steps

1. ✅ Review sequence diagrams with team
2. ⬜ Implement `CheckpointManager` class
3. ⬜ Add checkpointing to `function_app.py`
4. ⬜ Test with sample large files
5. ⬜ Enable feature flag for gradual rollout
6. ⬜ Monitor performance metrics
7. ⬜ Tune chunk sizes and retry logic

## Conclusion

The parquet-based checkpointing solution provides:
- **Fault tolerance** through granular chunk tracking
- **Fast resume** by skipping completed chunks
- **Observable progress** via manifest metadata
- **Cost efficiency** through parquet compression
- **Minimal overhead** on fresh starts

This architecture integrates seamlessly with the existing streaming pipeline while adding robust error recovery capabilities.
