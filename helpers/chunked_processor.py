"""
Chunked data processing and database operations for memory efficiency.
"""

import logging
import pandas as pd
import time
from typing import Tuple, Iterator, Optional
from helpers.validator import validate_ztdwr_data
from helpers.transformer import transform_to_transport_documents
from helpers.postgres_client import upsert_to_postgres


class ChunkedPipelineProcessor:
    """Process data in chunks through validation, transformation, and DB upsert."""

    def __init__(self, sync_id: str, file_name: str, error_threshold: float = 0.05):
        self.sync_id = sync_id
        self.file_name = file_name
        self.error_threshold = error_threshold

        # Accumulators
        self.total_rows = 0
        self.failed_rows = 0
        self.records_inserted = 0
        self.records_updated = 0
        self.validation_errors = []

        # Performance tracking
        self.chunk_count = 0
        self.start_time = time.time()
        self.last_chunk_time = time.time()

    def process_chunk(self, chunk_df: pd.DataFrame) -> Tuple[int, int]:
        """
        Process a single chunk: validate, transform, and upsert.

        Returns:
            (rows_inserted, rows_updated) for this chunk
        """
        self.chunk_count += 1
        chunk_size = len(chunk_df)
        self.total_rows += chunk_size

        # Calculate timing and progress
        current_time = time.time()
        chunk_duration = current_time - self.last_chunk_time
        total_duration = current_time - self.start_time
        avg_chunk_time = total_duration / self.chunk_count if self.chunk_count > 0 else 0
        rows_per_sec = self.total_rows / total_duration if total_duration > 0 else 0

        logging.info(
            f"📦 CHUNK {self.chunk_count}: Processing {chunk_size} rows "
            f"(total: {self.total_rows} rows, {rows_per_sec:.1f} rows/sec, "
            f"chunk took {chunk_duration:.1f}s)"
        )

        self.last_chunk_time = current_time

        # Step 1: Validate chunk
        chunk_validation_errors = validate_ztdwr_data(chunk_df)

        # Separate row-level and global errors
        row_level_errors = [e for e in chunk_validation_errors if isinstance(e, dict) and 'row' in e]
        global_errors = [e for e in chunk_validation_errors if not (isinstance(e, dict) and 'row' in e)]

        # If there are global errors, raise immediately
        if global_errors:
            error_msg = f"Global validation errors in chunk: {global_errors[0]}"
            self.validation_errors.extend(chunk_validation_errors)
            raise ValueError(error_msg)

        # Track validation errors
        if chunk_validation_errors:
            self.validation_errors.extend(chunk_validation_errors)
            failed_row_indices = {e['row'] for e in row_level_errors}
            chunk_failed = len(failed_row_indices)
            self.failed_rows += chunk_failed

            # Check if cumulative error rate exceeds threshold
            current_error_rate = self.failed_rows / self.total_rows
            if current_error_rate > self.error_threshold:
                raise ValueError(
                    f"Error rate {current_error_rate:.2%} exceeds threshold {self.error_threshold:.2%} "
                    f"(failed={self.failed_rows}, total={self.total_rows})"
                )

            # Remove invalid rows from chunk
            invalid_indices = sorted(failed_row_indices)
            chunk_df = chunk_df.drop(index=invalid_indices).reset_index(drop=True)
            logging.info(f"Removed {len(invalid_indices)} invalid rows from chunk")

        # If all rows in chunk were invalid, return early
        if len(chunk_df) == 0:
            logging.warning("All rows in chunk were invalid, skipping transformation and upsert")
            return (0, 0)

        # Step 2: Transform chunk
        transformed_chunk = transform_to_transport_documents(chunk_df, self.sync_id, self.file_name)

        # CRITICAL SAFETY CHECK: Filter out any rows with NULL primary keys that somehow got through
        # This is the last line of defense before database insert
        if 'surat_pengantar_barang' in transformed_chunk.columns:
            pk_series = transformed_chunk['surat_pengantar_barang']

            # Comprehensive NULL detection - check multiple conditions
            # Convert to string first to handle all types uniformly
            pk_str = pk_series.fillna('').astype(str).str.strip()

            null_pk_mask = (
                pk_series.isna() |  # pandas NA/NaN/None
                (pk_str == '') |  # Empty string after strip
                (pk_str == 'None') |
                (pk_str == 'nan') |
                (pk_str == 'NaN') |
                (pk_str == 'NAN') |
                (pk_str == '<NA>') |
                (pk_str == 'null') |
                (pk_str == 'NULL') |
                (pk_str == 'nat') |
                (pk_str == 'NaT') |
                (pk_str == 'NAT')
            )

            null_pk_count = null_pk_mask.sum()

            if null_pk_count > 0:
                logging.error(
                    f"❌ CRITICAL: Found {null_pk_count} NULL primary keys in transformed chunk {self.chunk_count} - FILTERING THEM OUT"
                )
                # Log samples for debugging
                null_pk_indices = transformed_chunk[null_pk_mask].index[:5].tolist()
                for idx in null_pk_indices[:3]:
                    pk_val = transformed_chunk.loc[idx, 'surat_pengantar_barang']
                    logging.error(f"  Row {idx}: surat_pengantar_barang='{pk_val}' (type: {type(pk_val).__name__})")

                # FORCE REMOVE these rows
                transformed_chunk = transformed_chunk[~null_pk_mask].copy()
                logging.error(f"  Removed {null_pk_count} rows with NULL primary keys from chunk")

                # Track as failed rows
                self.failed_rows += null_pk_count

        # Additional safety check: ensure we have data after filtering
        if len(transformed_chunk) == 0:
            logging.warning("Transformation returned empty DataFrame (all rows filtered), skipping upsert")
            return (0, 0)

        # Step 3: Upsert chunk to database
        try:
            chunk_inserted, chunk_updated = upsert_to_postgres(self.sync_id, transformed_chunk)
        except Exception as upsert_error:
            import sys
            logging.error(f"❌ UPSERT FAILED for chunk {self.chunk_count}: {upsert_error}", exc_info=True)
            sys.stdout.flush()
            sys.stderr.flush()
            # Re-raise to let the chunk processor handle it
            raise

        # Step 4: Upsert drivers (if nik_supir and nama_supir are present)
        try:
            from helpers.postgres_client import upsert_drivers
            drivers_inserted, drivers_updated = upsert_drivers(self.sync_id, transformed_chunk)
            if drivers_inserted > 0 or drivers_updated > 0:
                logging.info(f"  Drivers: {drivers_inserted} inserted, {drivers_updated} updated")
        except Exception as driver_error:
            # Don't fail the whole chunk if driver upsert fails
            logging.warning(f"⚠️  Driver upsert failed (non-fatal): {driver_error}")

        self.records_inserted += chunk_inserted
        self.records_updated += chunk_updated

        logging.info(
            f"✅ CHUNK {self.chunk_count} COMPLETE: +{chunk_inserted} inserted, +{chunk_updated} updated "
            f"(cumulative: {self.records_inserted} inserted, {self.records_updated} updated, "
            f"{self.failed_rows} failed)"
        )

        return (chunk_inserted, chunk_updated)

    def get_summary(self) -> dict:
        """Get processing summary statistics."""
        return {
            'total_rows': self.total_rows,
            'failed_rows': self.failed_rows,
            'records_inserted': self.records_inserted,
            'records_updated': self.records_updated,
            'validation_errors': self.validation_errors,
            'error_rate': self.failed_rows / self.total_rows if self.total_rows > 0 else 0
        }


def process_chunks_with_backpressure(
    chunk_iterator: Iterator[pd.DataFrame],
    processor: ChunkedPipelineProcessor,
    max_buffer_chunks: int = 2
) -> dict:
    """
    Process chunks with backpressure to limit memory usage.

    Args:
        chunk_iterator: Iterator yielding DataFrame chunks
        processor: ChunkedPipelineProcessor instance
        max_buffer_chunks: Maximum chunks to buffer (for backpressure)

    Returns:
        Processing summary dict
    """
    chunk_count = 0

    try:
        for chunk_df in chunk_iterator:
            chunk_count += 1

            # Process chunk immediately (no buffering beyond DB batch size)
            processor.process_chunk(chunk_df)

            # Explicit garbage collection every N chunks if needed
            if chunk_count % 10 == 0:
                import gc
                gc.collect()
                logging.debug(f"Garbage collection after {chunk_count} chunks")

        logging.info(f"Completed processing {chunk_count} chunks")
        return processor.get_summary()

    except Exception as e:
        logging.error(f"Error processing chunk {chunk_count}: {e}")
        # Still return partial summary for debugging
        summary = processor.get_summary()
        summary['error'] = str(e)
        summary['failed_at_chunk'] = chunk_count
        raise


def optimize_postgres_batch_size(chunk_size: int, num_columns: int) -> int:
    """
    Calculate optimal PostgreSQL batch size based on chunk size and columns.

    Args:
        chunk_size: Number of rows in a chunk
        num_columns: Number of columns in the data

    Returns:
        Optimal batch size for execute_values
    """
    # PostgreSQL has a limit on the number of parameters per query
    # Default is 65535, but we want to stay well below that
    max_params = 30000

    # Calculate max rows per batch
    max_rows_per_batch = max_params // num_columns

    # Use smaller of chunk_size and calculated max
    batch_size = min(chunk_size, max_rows_per_batch, 5000)

    # Ensure at least 100 rows per batch
    batch_size = max(100, batch_size)

    logging.debug(f"Calculated batch size: {batch_size} (chunk_size={chunk_size}, columns={num_columns})")
    return batch_size

