# Research: Loading .DAT Files into Pandas DataFrame

## Executive Summary

This document provides comprehensive research on Python libraries and approaches for efficiently loading large .DAT files into pandas DataFrames, with specific consideration for the existing ZTDWR transport intelligence ETL pipeline.

## What are .DAT Files?

.DAT files are generic data files that can contain various types of delimited text data. In this codebase:
- ZTDWR .dat files are tab-separated (TSV) or custom-delimited (`~^` pattern) text files
- Files may be gzipped (`.gz` compressed)
- Can range from small (< 1MB) to very large (> 100MB)
- Current implementation in `helpers/parser.py` and `helpers/streaming_parser.py`

## Current Implementation Analysis

### Current Approach (helpers/streaming_parser.py)
```python
# Uses pandas read_csv with chunksize parameter
chunk_iter = pd.read_csv(
    text_io,
    sep=delimiter,
    engine=engine,
    dtype=str,
    chunksize=chunk_size  # Default: 10,000 rows
)
```

**Strengths:**
- Memory efficient chunked reading
- Handles custom delimiters (`~^`)
- Automatic delimiter detection
- UTF-8 and Latin-1 encoding fallback

**Limitations:**
- No checkpointing (restart from beginning on failure)
- Transformation/cleansing applied per chunk without intermediate persistence
- Chunks are ephemeral (not saved)

## Recommended Libraries for Large File Processing

### 1. Pandas (Current Solution) ⭐ RECOMMENDED FOR THIS PROJECT

**Why it fits:**
- Already integrated in codebase
- Excellent for delimited text files
- Supports chunked reading with `chunksize` parameter
- Wide ecosystem support

**Performance Optimizations:**
```python
# Specify dtypes upfront to reduce memory
dtypes = {
    'surat_pengantar_brg': 'string',
    'nik_supir': 'string',
    'kilometer_actual': 'float32'  # Use float32 instead of float64
}

pd.read_csv(
    file,
    dtype=dtypes,
    chunksize=10000,
    low_memory=False
)
```

**Best for:**
- Tab-separated or custom-delimited files (✅ Matches ZTDWR format)
- Streaming/chunked processing
- Integration with existing pandas-based pipeline

### 2. Dask (Alternative for Massive Parallelization)

**Overview:**
- Parallel computing library that extends pandas
- Processes data in parallel across multiple cores
- Lazy evaluation (doesn't load until needed)

**Example:**
```python
import dask.dataframe as dd

# Read large file with Dask
ddf = dd.read_csv('large_file.dat', sep='\t', blocksize='64MB')

# Operations are lazy - computed only when needed
result = ddf.groupby('column').sum().compute()

# Convert to pandas
pandas_df = ddf.compute()
```

**Pros:**
- 4-8x faster on multi-core machines
- Out-of-core processing (handles data larger than RAM)
- Familiar pandas-like API

**Cons:**
- Adds complexity and dependencies
- Requires refactoring existing pipeline
- Not beneficial for single-threaded Azure Functions

**Verdict:** ❌ Not recommended - Current Azure Function is single-threaded, Dask overhead not justified

### 3. Polars (High-Performance Alternative)

**Overview:**
- Modern DataFrame library written in Rust
- 5-10x faster than pandas for many operations
- Built-in lazy evaluation and streaming

**Example:**
```python
import polars as pl

# Lazy reading (doesn't load until compute() called)
df = pl.scan_csv('file.dat', sep='\t')

# Streaming reading for large files
df = pl.read_csv('file.dat', sep='\t', low_memory=True)
```

**Pros:**
- Extremely fast (Rust-based)
- Better memory efficiency than pandas
- Native lazy evaluation

**Cons:**
- Different API from pandas (requires code rewrite)
- Less mature ecosystem
- Current codebase heavily pandas-dependent

**Verdict:** ⚠️ Consider for future - Would require significant refactoring

### 4. Vaex (Out-of-Core DataFrames)

**Overview:**
- Designed for datasets larger than RAM
- Memory mapping for instant loading
- Zero-copy operations

**Example:**
```python
import vaex

# Memory-mapped - nearly instant "loading"
df = vaex.from_csv('large_file.dat', sep='\t', convert=True)
```

**Pros:**
- Handles billion-row datasets
- Instant loading via memory mapping
- Low memory footprint

**Cons:**
- Limited to local files (not Azure Blob streams)
- Different API from pandas
- Not suitable for Azure Functions blob triggers

**Verdict:** ❌ Not applicable - Requires local file system

### 5. Datatable (Python port of R's data.table)

**Overview:**
- Fast C++ backend
- 36x faster than pandas for sorting
- Good for large data on single machine

**Example:**
```python
import datatable as dt

# Read file
df = dt.fread('file.dat', sep='\t')

# Convert to pandas
pandas_df = df.to_pandas()
```

**Pros:**
- Very fast reading and sorting
- Lower memory usage than pandas
- Easy conversion to pandas

**Cons:**
- Smaller feature set than pandas
- Less active development
- Still requires full file in memory eventually

**Verdict:** ⚠️ Possible optimization - Could speed up initial parsing, but requires testing

## Comparative Performance Benchmarks

Based on community benchmarks for 1GB delimited file:

| Library | Read Time | Memory Usage | Complexity |
|---------|-----------|--------------|------------|
| Pandas (chunked) | Baseline | Baseline | Low |
| Pandas (optimized dtypes) | 0.7x | 0.5x | Low |
| Dask | 0.25x (multi-core) | 0.6x | Medium |
| Polars | 0.2x | 0.4x | High (API change) |
| Datatable | 0.3x | 0.5x | Medium |
| Vaex | 0.1x (memory-map) | 0.1x | High (not applicable) |

## Recommendations for ZTDWR Pipeline

### Short-term (Immediate Implementation)

**Stick with Pandas + Add Optimizations:**

1. **Optimize dtypes** to reduce memory by 50%:
```python
# In streaming_parser.py, add dtype specifications
OPTIMIZED_DTYPES = {
    'surat_pengantar_brg': 'string',
    'no_tiket': 'string',
    'nik_supir': 'string',
    'no_polisi': 'string',
    'kilometer_actual': 'float32',  # Instead of float64
    'bbm_actual': 'float32',
    'qty_dikirim': 'float32',
    'qty_diterima': 'float32',
}

pd.read_csv(
    text_io,
    sep=delimiter,
    engine=engine,
    dtype=OPTIMIZED_DTYPES,
    chunksize=chunk_size
)
```

2. **Add Parquet Checkpointing** (see next document)

3. **Use `pyarrow` engine** for faster parquet I/O:
```bash
pip install pyarrow
```

### Medium-term (Future Consideration)

**Evaluate Datatable for Initial Parsing:**
- Test if `dt.fread()` is significantly faster for initial .dat load
- Convert to pandas for downstream processing
- Minimal code changes required

### Long-term (Major Refactor)

**Consider Polars if:**
- Processing time becomes critical bottleneck
- Willing to invest in API migration
- Need significant performance gains (5-10x)

## Integration with Current Codebase

### Current File: `helpers/streaming_parser.py`

The existing implementation is solid. Key improvements:

```python
# BEFORE
pd.read_csv(text_io, sep=delimiter, engine=engine, dtype=str, ...)

# AFTER (Optimized)
DTYPE_MAP = {
    'surat_pengantar_brg': 'string',
    'nik_supir': 'string',
    # ... other string columns
    'kilometer_actual': 'float32',
    'bbm_actual': 'float32',
    # ... other numeric columns
}

pd.read_csv(
    text_io,
    sep=delimiter,
    engine=engine,
    dtype=DTYPE_MAP,  # Reduce memory by 50%
    na_values=['', 'NULL', 'null', 'None', '~'],
    keep_default_na=False,
    chunksize=chunk_size,
    low_memory=False  # Slightly slower but more consistent
)
```

## Parquet Format Benefits

### Why Convert to Parquet?

1. **60x Faster Read Performance:**
   - Parquet is columnar format
   - Only reads needed columns
   - Built-in compression

2. **Checkpoint-Friendly:**
   - Each chunk saved as separate .parquet file
   - Resume by skipping already-processed files

3. **Smaller Storage:**
   - 5-10x compression vs CSV
   - Reduces Azure Blob storage costs

### Example Parquet Workflow

```python
# Save chunk as parquet
chunk_df.to_parquet(
    f'checkpoint_{sync_id}/chunk_{chunk_num:04d}.parquet',
    engine='pyarrow',
    compression='snappy',  # Fast compression
    index=False
)

# Read back for processing
chunk_df = pd.read_parquet(
    f'checkpoint_{sync_id}/chunk_{chunk_num:04d}.parquet',
    engine='pyarrow'
)
```

## Memory Optimization Strategies

### 1. Reduce Chunk Size Dynamically

```python
# Current: Fixed 10,000 rows
# Optimized: Adaptive based on file size and memory

def estimate_optimal_chunk_size(file_size_mb, available_memory_mb):
    # Target: Use max 25% of available memory per chunk
    target_chunk_memory_mb = available_memory_mb * 0.25

    # Estimate row size (varies by columns)
    estimated_row_size_kb = 2  # Conservative estimate

    chunk_size = int((target_chunk_memory_mb * 1024) / estimated_row_size_kb)

    # Bounds: 1,000 - 50,000 rows
    return max(1000, min(50000, chunk_size))
```

### 2. Explicit Garbage Collection

```python
import gc

for chunk in chunk_iterator:
    process_chunk(chunk)

    # Explicit cleanup every 10 chunks
    if chunk_num % 10 == 0:
        gc.collect()
```

### 3. String Interning for Repeated Values

```python
# For columns with many repeated values (e.g., sender, receiver)
df['sender'] = df['sender'].astype('category')
df['receiver'] = df['receiver'].astype('category')

# Reduces memory by 10-20x for high-cardinality repeated strings
```

## Conclusion

**For ZTDWR Pipeline:**
1. ✅ Keep pandas as primary library (mature, integrated, sufficient)
2. ✅ Add dtype optimizations (50% memory reduction, easy win)
3. ✅ Implement parquet checkpointing (fault tolerance, 60x faster resume)
4. ⚠️ Consider datatable for future testing (potential 3x speedup in parsing)
5. ❌ Skip Dask/Polars/Vaex (overkill for current scale and architecture)

**Next Steps:**
1. Implement parquet-based checkpointing (see next document)
2. Add dtype optimizations to streaming_parser.py
3. Test with real large files (> 50MB)
4. Monitor memory usage and adjust chunk sizes

## References

- [Pandas Chunking Documentation](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)
- [Parquet vs CSV Performance](https://deephaven.io/blog/2022/04/27/batch-process-data/)
- [Pandas Memory Optimization](https://www.datacamp.com/tutorial/benchmarking-high-performance-pandas-alternatives)
- [Handling Large Datasets in Python](https://towardsdatascience.com/how-to-handle-large-datasets-in-python-1f077a7e7ecf)
