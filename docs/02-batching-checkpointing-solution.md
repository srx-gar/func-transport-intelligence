# Batching and Checkpointing Solution for Large File ETL

## Problem Statement

### Current Pain Points

1. **Long Processing Time:** Large .dat files (>50MB) take significant time to:
   - Parse and load into pandas
   - Validate and cleanse data
   - Transform to target schema
   - Upsert to PostgreSQL

2. **No Fault Tolerance:** If processing fails at row 80,000 of 100,000:
   - Must restart from beginning
   - Wasted compute time
   - Wasted database operations (may have duplicate upserts)

3. **Memory Pressure:** Processing 100MB+ files can:
   - Exhaust Azure Function memory (1.5GB limit)
   - Cause timeouts (10-minute limit)
   - Trigger OOM errors

4. **No Progress Visibility:** Cannot determine:
   - How much progress was made before failure
   - Where to resume
   - Which chunks succeeded

### Current Architecture (Existing Codebase)

From `function_app.py:389-556` (`_run_streaming_pipeline`):

```
.dat file (Azure Blob)
    → Decompress if gzipped
    → Parse into chunks (10K rows each)
    → For each chunk:
        ├─ Validate
        ├─ Transform
        └─ Upsert to PostgreSQL
    → Mark sync complete
```

**Issue:** Chunks are ephemeral (processed once, not saved). Failure = start over.

## Proposed Solution: Parquet-Based Checkpointing

### Architecture Overview

```
.dat file (Azure Blob)
    → Step 1: Convert to Parquet Batches (ONCE)
    │   ├─ Decompress if gzipped
    │   ├─ Parse into chunks
    │   ├─ Save each chunk as chunk_0000.parquet, chunk_0001.parquet, ...
    │   └─ Create checkpoint manifest (metadata.json)
    │
    → Step 2: Process Parquet Batches (RESUMABLE)
        ├─ Read checkpoint manifest
        ├─ Skip already-processed chunks
        ├─ For each remaining chunk:
        │   ├─ Load from .parquet (60x faster than .dat)
        │   ├─ Validate
        │   ├─ Transform
        │   ├─ Upsert to PostgreSQL
        │   └─ Mark chunk as complete in manifest
        └─ Finalize sync
```

### Key Benefits

1. **Idempotent Conversion:** .dat → Parquet happens once
   - If Step 1 fails, can restart just Step 1
   - If Step 2 fails, restart from last successful chunk

2. **Fast Resume:** Reading .parquet is 60x faster than re-parsing .dat
   - Validation/transformation already has clean data
   - Skip completed chunks instantly

3. **Progress Tracking:** Know exactly which chunks succeeded
   - Manifest file tracks: `{chunk_id: status, timestamp}`
   - Can monitor progress in real-time

4. **Space Efficient:** Parquet is 5-10x smaller than .dat
   - Compression built-in
   - Temporary files cleaned up after success

## Detailed Design

### Checkpoint Manifest Structure

**File:** `checkpoints/{sync_id}/manifest.json`

```json
{
  "sync_id": "sync_20250129_143022_a3f4b2c1",
  "source_file": "ZTDWR_2025012 9_LARGE.dat",
  "source_path": "data/hex-ztdwr/ZTDWR_20250129_LARGE.dat",
  "file_size_bytes": 104857600,
  "total_chunks": 150,
  "chunk_size": 10000,
  "status": "processing",
  "created_at": "2025-01-29T14:30:22Z",
  "updated_at": "2025-01-29T14:45:10Z",
  "chunks": [
    {
      "chunk_id": 0,
      "status": "completed",
      "rows": 10000,
      "parquet_path": "chunk_0000.parquet",
      "parquet_size_bytes": 524288,
      "processed_at": "2025-01-29T14:32:15Z",
      "records_inserted": 9850,
      "records_updated": 150,
      "validation_errors": 0
    },
    {
      "chunk_id": 1,
      "status": "completed",
      "rows": 10000,
      "parquet_path": "chunk_0001.parquet",
      "parquet_size_bytes": 520192,
      "processed_at": "2025-01-29T14:33:42Z",
      "records_inserted": 9920,
      "records_updated": 80,
      "validation_errors": 0
    },
    {
      "chunk_id": 2,
      "status": "failed",
      "rows": 10000,
      "parquet_path": "chunk_0002.parquet",
      "parquet_size_bytes": 518144,
      "error": "Database connection timeout",
      "failed_at": "2025-01-29T14:35:00Z"
    },
    {
      "chunk_id": 3,
      "status": "pending",
      "rows": 10000,
      "parquet_path": "chunk_0003.parquet",
      "parquet_size_bytes": 522240
    }
  ]
}
```

### Storage Structure

```
Azure Blob Storage:
  data/
    hex-ztdwr/
      ZTDWR_20250129.dat              # Original file
    checkpoints/
      sync_20250129_143022_a3f4b2c1/  # Checkpoint directory
        manifest.json                  # Checkpoint manifest
        chunk_0000.parquet             # Chunk 0 (rows 0-9,999)
        chunk_0001.parquet             # Chunk 1 (rows 10,000-19,999)
        chunk_0002.parquet             # Chunk 2 (rows 20,000-29,999)
        ...
```

### Implementation Components

#### 1. Checkpoint Manager (`helpers/checkpoint_manager.py`)

```python
class CheckpointManager:
    """Manages parquet-based checkpoints for large file processing."""

    def __init__(self, sync_id: str, source_file: str, checkpoint_base_path: str):
        self.sync_id = sync_id
        self.checkpoint_dir = f"{checkpoint_base_path}/{sync_id}"
        self.manifest_path = f"{self.checkpoint_dir}/manifest.json"
        self.manifest = None

    def create_checkpoint(self, total_chunks: int, chunk_size: int, file_size: int):
        """Initialize checkpoint manifest."""
        self.manifest = {
            'sync_id': self.sync_id,
            'source_file': source_file,
            'total_chunks': total_chunks,
            'chunk_size': chunk_size,
            'file_size_bytes': file_size,
            'status': 'converting',
            'created_at': datetime.utcnow().isoformat(),
            'chunks': []
        }
        self._save_manifest()

    def save_chunk_parquet(self, chunk_id: int, chunk_df: pd.DataFrame):
        """Save chunk as parquet file."""
        parquet_path = f"{self.checkpoint_dir}/chunk_{chunk_id:04d}.parquet"

        # Save with optimal compression
        chunk_df.to_parquet(
            parquet_path,
            engine='pyarrow',
            compression='snappy',  # Fast compression (2x faster than gzip)
            index=False
        )

        # Update manifest
        chunk_info = {
            'chunk_id': chunk_id,
            'status': 'ready',
            'rows': len(chunk_df),
            'parquet_path': f"chunk_{chunk_id:04d}.parquet",
            'parquet_size_bytes': os.path.getsize(parquet_path),
            'created_at': datetime.utcnow().isoformat()
        }

        self.manifest['chunks'].append(chunk_info)
        self._save_manifest()

        logging.info(f"Saved chunk {chunk_id} as parquet: {len(chunk_df)} rows")

    def get_pending_chunks(self) -> List[int]:
        """Get list of chunk IDs that need processing."""
        return [
            chunk['chunk_id']
            for chunk in self.manifest['chunks']
            if chunk['status'] in ['ready', 'failed', 'pending']
        ]

    def mark_chunk_complete(self, chunk_id: int, inserted: int, updated: int):
        """Mark chunk as successfully processed."""
        for chunk in self.manifest['chunks']:
            if chunk['chunk_id'] == chunk_id:
                chunk['status'] = 'completed'
                chunk['processed_at'] = datetime.utcnow().isoformat()
                chunk['records_inserted'] = inserted
                chunk['records_updated'] = updated
                break

        self._save_manifest()

    def mark_chunk_failed(self, chunk_id: int, error: str):
        """Mark chunk as failed with error message."""
        for chunk in self.manifest['chunks']:
            if chunk['chunk_id'] == chunk_id:
                chunk['status'] = 'failed'
                chunk['failed_at'] = datetime.utcnow().isoformat()
                chunk['error'] = error
                break

        self._save_manifest()

    def load_chunk(self, chunk_id: int) -> pd.DataFrame:
        """Load chunk from parquet."""
        parquet_path = f"{self.checkpoint_dir}/chunk_{chunk_id:04d}.parquet"
        return pd.read_parquet(parquet_path, engine='pyarrow')

    def cleanup(self):
        """Remove checkpoint directory after successful completion."""
        # Delete all parquet files and manifest
        import shutil
        shutil.rmtree(self.checkpoint_dir)
        logging.info(f"Cleaned up checkpoint directory: {self.checkpoint_dir}")

    def _save_manifest(self):
        """Save manifest to JSON."""
        with open(self.manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)
```

#### 2. Modified Streaming Pipeline (`function_app.py`)

```python
def _run_streaming_pipeline_with_checkpoints(
    *,
    file_name: str,
    blob_path: str,
    raw_content: bytes,
    trigger: str,
    blob_size: Optional[int] = None,
) -> dict:
    """
    Execute streaming pipeline with parquet checkpointing.

    Two-phase approach:
    1. Convert .dat to parquet chunks (if not already done)
    2. Process parquet chunks (resumable)
    """

    sync_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)

    logging.info("=" * 80)
    logging.info("🚀 STARTING STREAMING PIPELINE WITH CHECKPOINTS")
    logging.info("=" * 80)

    checkpoint_base = os.getenv('CHECKPOINT_BASE_PATH', 'checkpoints')
    checkpoint_manager = CheckpointManager(sync_id, file_name, checkpoint_base)

    # PHASE 1: Convert .dat to Parquet Chunks
    # ===========================================
    if not checkpoint_manager.checkpoint_exists():
        logging.info("📦 PHASE 1: Converting .dat file to parquet chunks...")

        # Determine chunk size
        file_size = blob_size or len(raw_content)
        chunk_size = estimate_chunk_size(file_size, available_memory_mb=512)

        # Parse .dat file into chunks
        chunk_iterator = parse_ztdwr_chunks(
            raw_content,
            chunk_size=chunk_size,
            decompress=True
        )

        chunk_id = 0
        chunk_list = []

        for chunk_df in chunk_iterator:
            # Save chunk as parquet (fast write, columnar compression)
            checkpoint_manager.save_chunk_parquet(chunk_id, chunk_df)
            chunk_list.append(chunk_id)
            chunk_id += 1

            logging.info(f"✅ Converted chunk {chunk_id}/{chunk_id+1} to parquet")

        checkpoint_manager.finalize_conversion(total_chunks=len(chunk_list))
        logging.info(f"✅ PHASE 1 COMPLETE: Converted {len(chunk_list)} chunks to parquet")

    else:
        logging.info("♻️  PHASE 1 SKIPPED: Parquet checkpoints already exist")

    # PHASE 2: Process Parquet Chunks (Resumable)
    # ============================================
    logging.info("⚙️  PHASE 2: Processing parquet chunks...")

    pending_chunks = checkpoint_manager.get_pending_chunks()
    total_chunks = checkpoint_manager.get_total_chunks()
    completed_chunks = total_chunks - len(pending_chunks)

    logging.info(
        f"📊 Progress: {completed_chunks}/{total_chunks} chunks completed, "
        f"{len(pending_chunks)} remaining"
    )

    # Process each pending chunk
    processor = ChunkedPipelineProcessor(sync_id, file_name, error_threshold=0.05)

    for chunk_id in pending_chunks:
        try:
            logging.info(f"📦 Processing chunk {chunk_id + 1}/{total_chunks}...")

            # Load chunk from parquet (60x faster than parsing .dat)
            chunk_df = checkpoint_manager.load_chunk(chunk_id)

            # Process chunk (validate, transform, upsert)
            chunk_inserted, chunk_updated = processor.process_chunk(chunk_df)

            # Mark as complete
            checkpoint_manager.mark_chunk_complete(
                chunk_id,
                inserted=chunk_inserted,
                updated=chunk_updated
            )

            logging.info(
                f"✅ Chunk {chunk_id + 1}/{total_chunks} complete: "
                f"+{chunk_inserted} inserted, +{chunk_updated} updated"
            )

        except Exception as e:
            logging.error(f"❌ Chunk {chunk_id} failed: {e}")
            checkpoint_manager.mark_chunk_failed(chunk_id, str(e))

            # Decide: continue or abort?
            if should_abort_on_chunk_failure(e):
                raise

    # Finalize sync
    summary = processor.get_summary()

    # Cleanup checkpoint files after success
    if summary['failed_rows'] == 0:
        checkpoint_manager.cleanup()
        logging.info("🗑️  Checkpoint files cleaned up after successful sync")

    # Update sync metadata
    update_sync_metadata(
        sync_id=sync_id,
        file_name=file_name,
        status='SUCCESS' if summary['failed_rows'] == 0 else 'PARTIAL_SUCCESS',
        records_total=summary['total_rows'],
        records_inserted=summary['records_inserted'],
        records_updated=summary['records_updated'],
        records_failed=summary['failed_rows'],
        validation_errors=summary['validation_errors']
    )

    return {
        'sync_id': sync_id,
        'status': 'SUCCESS',
        'total_chunks': total_chunks,
        'chunks_processed': total_chunks - len(pending_chunks),
        **summary
    }
```

### Resume Logic

When a sync is retried after failure:

```python
# Retry workflow
if checkpoint_manager.checkpoint_exists(sync_id):
    # Resume from existing checkpoint
    pending = checkpoint_manager.get_pending_chunks()
    logging.info(f"♻️  Resuming sync {sync_id}: {len(pending)} chunks remaining")

    # Phase 1 already complete, skip to Phase 2
    for chunk_id in pending:
        process_chunk(chunk_id)
else:
    # Fresh start
    convert_dat_to_parquet()
    process_all_chunks()
```

### Error Handling Strategies

#### 1. Transient Errors (Retry)

```python
# Database timeout, network error → Retry chunk
try:
    upsert_to_postgres(transformed_df)
except psycopg2.OperationalError as e:
    if 'timeout' in str(e).lower():
        # Mark as 'failed', will retry on next run
        checkpoint_manager.mark_chunk_failed(chunk_id, str(e))
        logging.warning(f"Chunk {chunk_id} failed (transient), will retry")
        continue  # Continue to next chunk
```

#### 2. Data Quality Errors (Skip)

```python
# Validation errors → Skip invalid rows, mark chunk complete
validation_errors = validate_chunk(chunk_df)

if validation_errors:
    # Remove invalid rows
    chunk_df = remove_invalid_rows(chunk_df, validation_errors)

# Still mark as complete (even with partial data)
checkpoint_manager.mark_chunk_complete(chunk_id, ...)
```

#### 3. Fatal Errors (Abort)

```python
# Missing columns, corrupted data → Abort entire sync
if 'surat_pengantar_barang' not in chunk_df.columns:
    checkpoint_manager.mark_sync_failed("Missing primary key column")
    raise ValueError("Fatal error: cannot proceed")
```

## Integration with Current Codebase

### Files to Modify

1. **`helpers/checkpoint_manager.py`** (NEW)
   - CheckpointManager class
   - Manifest management
   - Parquet I/O

2. **`function_app.py`**
   - Add `_run_streaming_pipeline_with_checkpoints()` function
   - Modify blob trigger to choose pipeline based on config
   - Add resume logic for retries

3. **`helpers/chunked_processor.py`**
   - Already compatible
   - Minor changes to pass checkpoint_manager

4. **Environment Variables** (`local.settings.json`)
   ```json
   {
     "ENABLE_CHECKPOINTING": "true",
     "CHECKPOINT_BASE_PATH": "checkpoints",
     "CHECKPOINT_CLEANUP_ON_SUCCESS": "true",
     "CHECKPOINT_RETENTION_DAYS": "7"
   }
   ```

## Performance Analysis

### Scenario: 100MB .dat file, 150,000 rows, 150 chunks

#### Without Checkpointing (Current)

| Event | Time | Cumulative |
|-------|------|------------|
| Parse .dat | 120s | 120s |
| Process all chunks | 300s | 420s |
| **FAILURE at chunk 120** | - | **420s WASTED** |
| **Retry: Re-parse .dat** | 120s | 540s |
| **Retry: Process chunks 0-120 again** | 240s | 780s |
| Process chunks 121-150 | 60s | 840s |
| **Total** | **14 minutes** | |

#### With Checkpointing (Proposed)

| Event | Time | Cumulative |
|-------|------|------------|
| Parse .dat → Parquet | 120s | 120s |
| Process chunks 0-119 | 240s | 360s |
| **FAILURE at chunk 120** | - | **360s** |
| **Retry: Load manifest** | 1s | 361s |
| **Retry: Skip chunks 0-119** | 0s | 361s |
| Process chunks 120-150 | 62s | 423s |
| **Total** | **7 minutes (50% faster)** | |

**Savings:** 7 minutes saved on retry + instant resume

### Memory Benefits

| Approach | Peak Memory | Notes |
|----------|-------------|-------|
| Current (streaming) | ~800MB | Holds chunk + transformed data |
| With checkpointing | ~600MB | Parquet is pre-parsed, less overhead |
| With checkpointing + dtype optimization | ~400MB | 50% reduction from optimized dtypes |

## Migration Path

### Phase 1: Implement Checkpointing (Week 1-2)

1. Create `helpers/checkpoint_manager.py`
2. Add `_run_streaming_pipeline_with_checkpoints()` to `function_app.py`
3. Add feature flag: `ENABLE_CHECKPOINTING=false` (default off)
4. Test with sample large files

### Phase 2: Gradual Rollout (Week 3-4)

1. Enable for files > 50MB
2. Monitor performance and error rates
3. Adjust chunk sizes and retry logic

### Phase 3: Full Migration (Week 5+)

1. Enable for all files > 20MB
2. Remove old streaming pipeline
3. Make checkpointing default

## Monitoring and Observability

### Metrics to Track

1. **Checkpoint Efficiency:**
   - Time saved on resume vs fresh start
   - Number of chunks skipped on resume

2. **Storage Usage:**
   - Checkpoint directory size
   - Parquet compression ratio
   - Cleanup success rate

3. **Failure Recovery:**
   - Resume success rate
   - Time to resume after failure
   - Chunks processed per sync

### Example Logging

```python
logging.info(
    "📊 Checkpoint Stats: "
    f"total_chunks={total_chunks}, "
    f"completed={completed_chunks}, "
    f"pending={len(pending_chunks)}, "
    f"parquet_size_mb={checkpoint_size_mb:.2f}, "
    f"resume_time_saved_sec={time_saved:.1f}"
)
```

## Alternative Approaches Considered

### 1. Database-Based Checkpointing

**Approach:** Track chunk status in `sync_metadata` table

**Pros:**
- No extra file storage
- Centralized state

**Cons:**
- Still need to re-parse .dat on resume (slow)
- Database becomes bottleneck
- More complex state management

**Verdict:** ❌ Rejected - Slower than parquet approach

### 2. Azure Durable Functions

**Approach:** Use Durable Functions for orchestration

**Pros:**
- Built-in checkpointing
- Automatic retries

**Cons:**
- Requires architecture change
- More expensive (longer execution time)
- Overkill for this use case

**Verdict:** ⚠️ Future consideration for complex workflows

### 3. Message Queue (Azure Queue)

**Approach:** Split into chunk messages, process via queue

**Pros:**
- Natural retry mechanism
- Parallel processing

**Cons:**
- Still need parquet for fast resume
- Added complexity
- Queue management overhead

**Verdict:** ⚠️ Complementary approach (can combine with parquet)

## Security Considerations

1. **Checkpoint Data Sensitivity:**
   - Parquet files contain same sensitive data as .dat
   - Use same access controls as source data
   - Enable blob encryption at rest

2. **Cleanup Policy:**
   - Delete checkpoints after 7 days (configurable)
   - Implement cleanup job for stale checkpoints

3. **Manifest Integrity:**
   - Validate manifest schema on load
   - Handle corrupted manifests gracefully

## Cost Analysis

### Storage Costs

- **Parquet files:** 10-20% of original .dat size (compression)
- **Retention:** 7 days (configurable)
- **Example:** 100MB .dat → 15MB parquet → ~$0.0003/day in Azure Blob (Hot tier)

**Monthly cost for 100 large files:** ~$0.90/month

### Compute Savings

- **Fewer retries:** 50% time saved on resume = 50% compute cost saved
- **Faster processing:** Parquet reading is 60x faster = less function execution time

**ROI:** Compute savings >> Storage costs

## Conclusion

### Summary of Benefits

1. ✅ **Fault Tolerant:** Resume from last successful chunk
2. ✅ **60x Faster Resume:** Parquet vs re-parsing .dat
3. ✅ **Memory Efficient:** 50% reduction with dtype + parquet
4. ✅ **Observable:** Clear progress tracking and metrics
5. ✅ **Cost Effective:** Minimal storage cost, significant compute savings

### Recommended Implementation

1. Implement `CheckpointManager` class
2. Add parquet-based checkpointing to streaming pipeline
3. Enable for files > 20MB
4. Monitor and tune chunk sizes
5. Automate checkpoint cleanup after 7 days

### Next Steps

1. See `03-implementation-sequence-diagram.md` for detailed flow
2. Review implementation plan with team
3. Create feature branch for development
4. Test with sample large files (50MB, 100MB, 200MB)
5. Gradual rollout with feature flag

## References

- [Checkpoint Pattern in ETL](https://thedataspartan.medium.com/lost-in-the-etl-multiverse-checkpoints-can-save-you-4d33d809f431)
- [Parquet Format Benefits](https://deephaven.io/blog/2022/04/27/batch-process-data/)
- [Azure Functions Best Practices](https://learn.microsoft.com/en-us/azure/azure-functions/functions-best-practices)
- [Pandas to Parquet Performance](https://www.sparkcodehub.com/pandas-dataframe-to-parquet)
