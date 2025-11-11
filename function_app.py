"""Azure Functions entrypoints for the transport intelligence ETL."""

import gzip
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional
import sys

# Disable Azure SDK HTTP logging BEFORE importing Azure modules
os.environ['AZURE_LOG_LEVEL'] = 'CRITICAL'
os.environ['AZURE_CORE_TRACING'] = 'false'
os.environ['AZURE_SDK_TRACING_IMPLEMENTATION'] = 'none'

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

# Suppress Azure SDK HTTP logging at the earliest possible point
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.CRITICAL)
logging.getLogger('azure.core.pipeline').setLevel(logging.CRITICAL)
logging.getLogger('azure.core').setLevel(logging.CRITICAL)

from helpers.notifier import send_alert_email
from helpers.parser import is_gzipped, parse_ztdwr_file
from helpers.postgres_client import (
    get_postgres_connection,
    refresh_materialized_views,
    update_sync_metadata,
    upsert_to_postgres,
)
from helpers.transformer import transform_to_transport_documents
from helpers.validator import validate_ztdwr_data

# Streaming optimization imports
from helpers.streaming_parser import parse_ztdwr_chunks, estimate_chunk_size
from helpers.chunked_processor import ChunkedPipelineProcessor, process_chunks_with_backpressure
from helpers.checkpoint_manager import CheckpointManager

# Polars high-performance parser imports
try:
    from helpers.polars_parser import parse_ztdwr_file_polars, polars_to_pandas
    from helpers.polars_streaming import parse_ztdwr_chunks_polars, estimate_chunk_size_polars
    POLARS_AVAILABLE = True
    logging.info("🚀 Polars parser available - high-performance mode enabled")
except ImportError:
    # Define fallback functions when Polars is not available
    parse_ztdwr_file_polars = None
    polars_to_pandas = None
    parse_ztdwr_chunks_polars = None
    estimate_chunk_size_polars = None
    POLARS_AVAILABLE = False
    logging.info("ℹ️  Polars not available - using standard Pandas parser")

# Configure logging BEFORE creating FunctionApp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Get or create root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Ensure we have a stdout handler
if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))
    root_logger.addHandler(handler)

app = func.FunctionApp()

dev_level = os.getenv('DEV_LOG_LEVEL', '').upper()
if dev_level == 'DEBUG':
    root_logger.setLevel(logging.DEBUG)
    # When in debug mode, keep Azure worker logs verbose
    logging.getLogger('azure').setLevel(logging.DEBUG)
    logging.getLogger('azure.functions_worker').setLevel(logging.DEBUG)
    logging.getLogger('azure.functions').setLevel(logging.DEBUG)
else:
    # Default to INFO to reduce noisy logs in normal runs
    root_logger.setLevel(logging.INFO)

    # Get Azure SDK log level from config (default: WARNING)
    azure_log_level = os.getenv('AZURE_LOG_LEVEL', 'WARNING').upper()
    azure_level = getattr(logging, azure_log_level, logging.WARNING)

    # Quiet noisy Azure SDK / HTTP logs during normal runs
    logging.getLogger('azure').setLevel(azure_level)
    logging.getLogger('azure.functions_worker').setLevel(logging.WARNING)
    logging.getLogger('azure.functions').setLevel(logging.WARNING)

    # Suppress Azure Storage SDK HTTP request/response logs (the noisy ones)
    logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.CRITICAL)
    logging.getLogger('azure.core.pipeline.policies').setLevel(logging.CRITICAL)
    logging.getLogger('azure.core.pipeline').setLevel(logging.CRITICAL)
    logging.getLogger('azure.core').setLevel(logging.CRITICAL)
    logging.getLogger('azure.storage').setLevel(logging.CRITICAL)
    logging.getLogger('azure.storage.blob').setLevel(logging.CRITICAL)
    logging.getLogger('azure.storage.blob._blob_client').setLevel(logging.CRITICAL)
    logging.getLogger('azure.storage.blob._download').setLevel(logging.CRITICAL)
    logging.getLogger('azure.storage.queue').setLevel(logging.CRITICAL)
    logging.getLogger('azure.storage.queue._queue_client').setLevel(logging.CRITICAL)
    logging.getLogger('azure.identity').setLevel(logging.CRITICAL)

    # Also reduce urllib3/requests noise (HTTP client traces)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.WARNING)


def _get_parser_info():
    """Get information about which parser will be used."""
    use_polars = os.getenv('USE_POLARS_PARSER', 'true').lower() == 'true'

    if use_polars and POLARS_AVAILABLE:
        return 'polars', '🚀 Polars (high-performance)'
    elif POLARS_AVAILABLE:
        return 'pandas', '🐼 Pandas (Polars disabled by config)'
    else:
        return 'pandas', '🐼 Pandas (Polars not installed)'


def _parse_dat_file(content: bytes):
    """
    Parse DAT file using Polars (if available and enabled) or Pandas.

    Environment variable: USE_POLARS_PARSER=true/false (default: true)

    Returns Pandas DataFrame for backward compatibility with downstream code.
    """
    use_polars = os.getenv('USE_POLARS_PARSER', 'true').lower() == 'true'

    if use_polars and POLARS_AVAILABLE:
        logging.info("🚀 Using Polars parser (high-performance mode)")
        df_polars = parse_ztdwr_file_polars(content)

        # Log performance metrics
        memory_mb = df_polars.estimated_size('mb')
        logging.info(f"📊 Polars DataFrame: {len(df_polars):,} rows, {memory_mb:.2f} MB")

        # Convert to Pandas for backward compatibility with transformer/validator
        logging.debug("Converting Polars → Pandas for downstream compatibility")
        return polars_to_pandas(df_polars)
    else:
        # Use standard Pandas parser
        logging.info("🐼 Using Pandas parser (standard mode)")
        return parse_ztdwr_file(content)


def _parse_dat_chunks(content: bytes, chunk_size: int, decompress: bool = True):
    """
    Parse DAT file in chunks using Polars (if available) or Pandas.

    Returns iterator of Pandas DataFrames for backward compatibility.
    """
    use_polars = os.getenv('USE_POLARS_PARSER', 'true').lower() == 'true'

    if use_polars and POLARS_AVAILABLE:
        logging.info("🚀 Using Polars streaming parser (memory-efficient mode)")

        # Parse with Polars and convert chunks to Pandas
        for chunk_polars in parse_ztdwr_chunks_polars(content, chunk_size=chunk_size, decompress=decompress):
            # Convert to Pandas for downstream compatibility
            yield polars_to_pandas(chunk_polars)
    else:
        # Use standard Pandas streaming parser
        logging.info("🐼 Using Pandas streaming parser")
        yield from parse_ztdwr_chunks(content, chunk_size=chunk_size, decompress=decompress)


def _estimate_chunk_size(file_size_bytes: int, available_memory_mb: int = 512) -> int:
    """
    Estimate optimal chunk size using Polars (if available) or Pandas logic.
    """
    use_polars = os.getenv('USE_POLARS_PARSER', 'true').lower() == 'true'

    if use_polars and POLARS_AVAILABLE:
        return estimate_chunk_size_polars(file_size_bytes, available_memory_mb)
    else:
        return estimate_chunk_size(file_size_bytes, available_memory_mb)


def _run_sync_pipeline(
        *,
        file_name: str,
        blob_path: str,
        raw_content: bytes,
        trigger: str,
        blob_size: Optional[int] = None,
) -> dict:
    """Execute the end-to-end sync pipeline and return a summary result."""

    sync_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)

    logging.info("=" * 80)
    logging.info("🚀 STARTING STANDARD PIPELINE")
    logging.info("=" * 80)
    logging.info(
        "Sync ID: %s | File: %s | Size: %s bytes | Trigger: %s",
        sync_id,
        file_name,
        blob_size,
        trigger,
    )

    # Persist initial sync metadata
    update_sync_metadata(
        sync_id=sync_id,
        file_name=file_name,
        file_path=blob_path,
        file_size_bytes=blob_size or len(raw_content),
        status='IN_PROGRESS',
    )

    try:
        # Step 1: Decompress if the payload is gzipped
        if is_gzipped(raw_content):
            logging.info("🗜️  Decompressing gzipped payload (%s bytes)...", len(raw_content))
            decompress_start = datetime.now(timezone.utc)
            content = gzip.decompress(raw_content)
            decompress_duration = (datetime.now(timezone.utc) - decompress_start).total_seconds()
            logging.info(
                "✅ Decompressed payload from %s bytes to %s bytes in %.2f seconds",
                len(raw_content),
                len(content),
                decompress_duration,
            )
        else:
            content = raw_content
            logging.info("Payload is plain text (%s bytes)", len(content))

        # Defensive quick-fix: sanitize the header line (first line) to remove
        # stray delimiter artefacts that sometimes appear in source files
        # (tokens like '^colname~' or 'colname~' or '\\' escaping). This only
        # operates on the first line and preserves the rest of the payload.
        try:
            if isinstance(content, (bytes, bytearray)):
                # work with bytes to avoid encoding assumptions
                parts = content.split(b"\n", 1)
                header = parts[0]
                rest = parts[1] if len(parts) > 1 else b''
                # split on pipe which is the inner separator of the ~|^ pattern
                header_tokens = header.split(b"|")
                cleaned_tokens = []
                for t in header_tokens:
                    s = t.replace(b"\\", b"")
                    s = s.strip()
                    # remove leading ^ characters and trailing ~ characters
                    while s.startswith(b"^"):
                        s = s[1:]
                    while s.endswith(b"~"):
                        s = s[:-1]
                    cleaned_tokens.append(s)
                new_header = b"|".join(cleaned_tokens)
                content = new_header + b"\n" + rest
                logging.info("Sanitized header (preview): %s", new_header[:200])
            else:
                # text str - convert to string first if needed
                text_content = content if isinstance(content, str) else content.decode('utf-8')
                parts = text_content.split('\n', 1)
                header = parts[0]
                rest = parts[1] if len(parts) > 1 else ''
                header_tokens = header.split('|')
                cleaned_tokens = []
                for t in header_tokens:
                    s = t.replace('\\', '')
                    s = s.strip()
                    s = s.lstrip('^').rstrip('~')
                    cleaned_tokens.append(s)
                new_header = '|'.join(cleaned_tokens)
                content = (new_header + '\n' + rest).encode('utf-8')
                logging.info("Sanitized header (preview): %s", new_header[:200])
        except Exception:
            logging.exception('Failed to sanitize header; continuing with original content')

        # Step 2: Parse raw text into a DataFrame
        parser_type, parser_desc = _get_parser_info()
        logging.info("📄 Parsing file content (%s bytes) with %s...", len(content), parser_desc)
        parse_start = datetime.now(timezone.utc)
        df = _parse_dat_file(content)
        total_rows = len(df)
        parse_duration = (datetime.now(timezone.utc) - parse_start).total_seconds()
        logging.info("✅ Parsed %s rows from %s in %.2f seconds using %s",
                     total_rows, file_name, parse_duration, parser_type)

        # Step 3: Validate source data
        logging.info("✔️  Validating %s rows...", total_rows)
        validation_start = datetime.now(timezone.utc)
        validation_errors = validate_ztdwr_data(df)
        validation_duration = (datetime.now(timezone.utc) - validation_start).total_seconds()
        logging.info("✅ Validation complete in %.2f seconds", validation_duration)

        # Split validation results into row-level errors and global errors
        row_level_errors = [e for e in validation_errors if isinstance(e, dict) and 'row' in e]
        global_errors = [e for e in validation_errors if not (isinstance(e, dict) and 'row' in e)]

        # Count unique failed row (row-level) for error rate calculation
        failed_row_indices = {e['row'] for e in row_level_errors}
        failed_rows_count = len(failed_row_indices)

        # Diagnostic logging: show a brief sample of validation errors to aid debugging
        try:
            logging.debug("Validation summary: total_rows=%s, row_level_errors=%s, global_errors=%s",
                          total_rows, len(row_level_errors), len(global_errors))
            if row_level_errors:
                logging.debug("Row-level error sample: %s", row_level_errors[:5])
            if global_errors:
                logging.debug("Global error sample: %s", global_errors[:5])
        except Exception:
            logging.exception('Failed to log validation diagnostic info')

        # If there are global errors (e.g., missing columns), treat them as fatal
        if global_errors:
            first_msg = None
            try:
                first_msg = global_errors[0].get('message', str(global_errors[0])) if isinstance(global_errors[0], dict) else str(global_errors[0])
            except Exception:
                first_msg = str(global_errors[0])

            error_msg = (
                f"Validation failed due to global errors: {first_msg} (global_errors_count={len(global_errors)}, total_rows={total_rows})"
            )
            logging.error("Validation stopped: %s; global_errors=%s", error_msg, global_errors[:5])
            # If there are also row-level errors, use PARTIAL_SUCCESS_BAD_INPUT
            if failed_rows_count > 0:
                status = 'PARTIAL_SUCCESS_BAD_INPUT'
            else:
                status = 'BAD_INPUT'
            update_sync_metadata(
                sync_id=sync_id,
                file_name=file_name,
                file_path=blob_path,
                file_size_bytes=blob_size or len(raw_content),
                status=status,
                records_total=total_rows,
                records_failed=failed_rows_count + len(global_errors),
                error_message=error_msg,
                validation_errors=validation_errors,
            )
            send_alert_email(sync_id, file_name, error_msg, validation_errors)
            raise ValueError(error_msg)

        error_rate = failed_rows_count / total_rows if total_rows else 0

        total_issues = failed_rows_count
        if total_issues:
            logging.warning(
                "Validation detected %s row-level issues (rate %.2f%%)",
                total_issues,
                error_rate * 100,
            )

            threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '1.0'))
            if error_rate > threshold:
                # Compose a more informative error message including counts and a small sample of row errors
                sample_errors = row_level_errors[:5]
                error_msg = (
                    f"Validation error rate {error_rate:.2%} exceeds threshold {threshold:.2%} "
                    f"(failed_rows={failed_rows_count}, total_rows={total_rows}). Sample errors: {sample_errors}"
                )
                update_sync_metadata(
                    sync_id=sync_id,
                    file_name=file_name,
                    file_path=blob_path,
                    file_size_bytes=blob_size or len(raw_content),
                    status='FAILED',
                    records_total=total_rows,
                    records_failed=total_issues,
                    error_message=error_msg,
                    validation_errors=validation_errors,
                )
                logging.error("Validation threshold exceeded: %s", error_msg)
                send_alert_email(sync_id, file_name, error_msg, validation_errors)
                raise ValueError(error_msg)

            invalid_indices = sorted(failed_row_indices)
            df = df.drop(index=invalid_indices)
            logging.info(
                "Continuing with %s valid rows after excluding %s invalid",
                len(df),
                len(invalid_indices),
            )

        # Step 4: Transform into target schema
        logging.info("🔄 Transforming %s rows to target schema...", len(df))
        transform_start = datetime.now(timezone.utc)
        transformed_df = transform_to_transport_documents(df, sync_id, file_name)
        transform_duration = (datetime.now(timezone.utc) - transform_start).total_seconds()
        logging.info("✅ Transformation complete in %.2f seconds, produced %s columns",
                     transform_duration, len(transformed_df.columns))

        # Step 5: Load into PostgreSQL
        logging.info("💾 Loading %s rows into PostgreSQL...", len(transformed_df))
        upsert_start = datetime.now(timezone.utc)
        records_inserted, records_updated = upsert_to_postgres(sync_id, transformed_df)
        upsert_duration = (datetime.now(timezone.utc) - upsert_start).total_seconds()
        logging.info("✅ Database upsert complete in %.2f seconds (inserted=%s, updated=%s)",
                     upsert_duration, records_inserted, records_updated)

        # Step 5.5: Upsert drivers to drivers table
        try:
            from helpers.postgres_client import upsert_drivers
            drivers_inserted, drivers_updated = upsert_drivers(sync_id, transformed_df)
            if drivers_inserted > 0 or drivers_updated > 0:
                logging.info(f"✅ Drivers upserted: {drivers_inserted} inserted, {drivers_updated} updated")
        except Exception as driver_error:
            logging.warning(f"⚠️  Driver upsert failed (non-fatal): {driver_error}")

        # Step 6: Refresh dependent materialized views
        try:
            if os.getenv('ENABLE_MV_REFRESH', 'true').lower() == 'true':
                refresh_materialized_views()
                logging.info("Materialized views refreshed")
            else:
                logging.info("Materialized view refresh skipped via config")
        except Exception as mv_error:
            logging.error(f"❌ Materialized view refresh failed: {mv_error}", exc_info=True)
            # Continue to cache prepopulation even if MV refresh fails

        # Step 7: Trigger cache prepopulation (always runs at end of pipeline)
        if os.getenv('ENABLE_CACHE_PREPOPULATION', 'true').lower() == 'true':
            from helpers.cache_prepopulator import trigger_cache_repopulation_safe
            logging.info("Triggering cache prepopulation...")
            success = trigger_cache_repopulation_safe(
                clear_first=True,
                concurrency=15
            )
            if success:
                logging.info("Cache prepopulation job started successfully")
            else:
                logging.warning("Cache prepopulation trigger failed (non-critical, continuing...)")
        else:
            logging.info("Cache prepopulation skipped via config")

        # Finalize status and record counts
        records_failed = failed_rows_count
        # If there were global errors, this would have already raised above
        # Check if failed rows are due to validation errors (bad input)
        if records_failed > 0 and validation_errors:
            status = 'PARTIAL_SUCCESS_BAD_INPUT'
        elif records_failed > 0:
            status = 'PARTIAL_SUCCESS'
        else:
            status = 'SUCCESS'
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            file_path=blob_path,
            file_size_bytes=blob_size or len(raw_content),
            status=status,
            records_total=total_rows,
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_failed=records_failed,
            validation_errors=validation_errors if validation_errors else None,
        )

        if validation_errors:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with {records_failed} validation errors",
                validation_errors,
                is_warning=True,
            )

        # Calculate performance metrics
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()
        duration_minutes = duration_seconds / 60
        rows_per_minute = total_rows / duration_minutes if duration_minutes > 0 else 0
        rows_per_second = total_rows / duration_seconds if duration_seconds > 0 else 0

        logging.info(
            "Sync %s completed (%s): inserted=%s updated=%s errors=%s | "
            "Performance: %s rows in %.2f min (%.0f rows/min, %.1f rows/sec)",
            sync_id,
            status,
            records_inserted,
            records_updated,
            records_failed,
            total_rows,
            duration_minutes,
            rows_per_minute,
            rows_per_second,
        )

        result = {
            'sync_id': sync_id,
            'status': status,
            'trigger': trigger,
            'file_name': file_name,
            'file_path': blob_path,
            'records_total': total_rows,
            'records_inserted': records_inserted,
            'records_updated': records_updated,
            'records_failed': records_failed,
            'duration_seconds': round(duration_seconds, 2),
            'duration_minutes': round(duration_minutes, 2),
            'rows_per_minute': round(rows_per_minute, 2),
            'rows_per_second': round(rows_per_second, 2),
        }

        # Print a single-line machine-readable summary to stdout so it's easy to
        # find in Functions host logs (useful when host suppresses verbose app logs).
        try:
            summary = {
                'sync_id': sync_id,
                'status': status,
                'file_name': file_name,
                'records_total': total_rows,
                'records_inserted': records_inserted,
                'records_updated': records_updated,
                'records_failed': records_failed,
            }
            print('SYNC_SUMMARY ' + json.dumps(summary, default=str))
        except Exception:
            logging.exception('Failed to print SYNC_SUMMARY')

        return result

    except Exception as exc:
        logging.error("Sync %s failed: %s", sync_id, exc, exc_info=True)
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            file_path=blob_path,
            file_size_bytes=blob_size or len(raw_content),
            status='FAILED',
            error_message=str(exc),
        )
        send_alert_email(sync_id, file_name, str(exc))
        raise


def _run_streaming_pipeline(
        *,
        file_name: str,
        blob_path: str,
        raw_content: bytes,
        trigger: str,
        blob_size: Optional[int] = None,
) -> dict:
    """
    Execute streaming pipeline for large files.
    Processes files in chunks to minimize memory usage and prevent timeouts.
    """

    sync_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)

    logging.info("=" * 80)
    logging.info("🚀 STARTING STREAMING PIPELINE")
    logging.info("=" * 80)
    logging.info(
        "Sync ID: %s | File: %s | Size: %.2f MB | Trigger: %s",
        sync_id,
        file_name,
        (blob_size or len(raw_content)) / 1024 / 1024,
        trigger,
    )

    # Persist initial sync metadata
    update_sync_metadata(
        sync_id=sync_id,
        file_name=file_name,
        file_path=blob_path,
        file_size_bytes=blob_size or len(raw_content),
        status='IN_PROGRESS',
    )

    try:
        # Determine optimal chunk size based on file size
        file_size = blob_size or len(raw_content)
        available_memory_mb = int(os.getenv('MAX_MEMORY_MB', '512'))
        chunk_size = _estimate_chunk_size(file_size, available_memory_mb)

        # Estimate total chunks
        estimated_rows = file_size // 1000  # Rough estimate: 1KB per row
        estimated_chunks = max(1, estimated_rows // chunk_size)

        parser_type, parser_desc = _get_parser_info()
        logging.info(f"📊 Using {parser_desc}")
        logging.info(f"📊 Chunk size: {chunk_size} rows | Estimated {estimated_chunks} chunks")

        # Get error threshold from config
        error_threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '1.0'))

        # Create chunked processor
        processor = ChunkedPipelineProcessor(sync_id, file_name, error_threshold)

        logging.info("🔄 Creating chunk iterator and starting processing...")

        # Create chunk iterator - will decompress automatically if needed
        chunk_iterator = _parse_dat_chunks(
            raw_content,
            chunk_size=chunk_size,
            decompress=True
        )

        # Process chunks with backpressure control
        max_buffer = int(os.getenv('MAX_BUFFER_CHUNKS', '2'))

        logging.info("⚙️  Processing chunks with backpressure control...")

        summary = process_chunks_with_backpressure(
            chunk_iterator,
            processor,
            max_buffer_chunks=max_buffer
        )

        # Check for processing errors
        if 'error' in summary:
            raise RuntimeError(summary['error'])

        # Extract results
        total_rows = summary['total_rows']
        records_inserted = summary['records_inserted']
        records_updated = summary['records_updated']
        records_failed = summary['failed_rows']
        validation_errors = summary['validation_errors']

        # Split validation results into row-level errors and global errors
        row_level_errors = [e for e in validation_errors if isinstance(e, dict) and 'row' in e]
        global_errors = [e for e in validation_errors if not (isinstance(e, dict) and 'row' in e)]
        failed_rows_count = len({e['row'] for e in row_level_errors})

        # Diagnostic logging for status determination
        logging.info(
            f"📊 Validation summary: {len(validation_errors)} total errors "
            f"({len(global_errors)} global, {len(row_level_errors)} row-level), "
            f"{failed_rows_count} failed rows"
        )

        # Determine status
        if global_errors and failed_rows_count > 0:
            status = 'PARTIAL_SUCCESS_BAD_INPUT'
        elif global_errors:
            status = 'BAD_INPUT'
        elif failed_rows_count > 0:
            # Check if failed rows are due to validation errors (bad input)
            if row_level_errors:
                status = 'PARTIAL_SUCCESS_BAD_INPUT'
            else:
                status = 'PARTIAL_SUCCESS'
        else:
            status = 'SUCCESS'

        # Update final metadata
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            file_path=blob_path,
            file_size_bytes=blob_size or len(raw_content),
            status=status,
            records_total=total_rows,
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_failed=records_failed,
            error_message=global_errors[0].get('message', str(global_errors[0])) if global_errors and isinstance(global_errors[0], dict) else (str(global_errors[0]) if global_errors else None),
            validation_errors=validation_errors if validation_errors else None,
        )

        # Send alerts if needed
        if global_errors:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with bad input data: {global_errors[0].get('message', str(global_errors[0])) if global_errors and isinstance(global_errors[0], dict) else (str(global_errors[0]) if global_errors else None)}",
                validation_errors,
                is_warning=True,
            )
        elif validation_errors:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with {records_failed} validation errors",
                validation_errors,
                is_warning=True,
            )

        # Calculate performance metrics
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()
        duration_minutes = duration_seconds / 60
        rows_per_minute = total_rows / duration_minutes if duration_minutes > 0 else 0
        rows_per_second = total_rows / duration_seconds if duration_seconds > 0 else 0

        logging.info(
            "Streaming sync %s completed (%s): inserted=%s updated=%s errors=%s | "
            "Performance: %s rows in %.2f min (%.0f rows/min, %.1f rows/sec)",
            sync_id,
            status,
            records_inserted,
            records_updated,
            records_failed,
            total_rows,
            duration_minutes,
            rows_per_minute,
            rows_per_second,
        )

        result = {
            'sync_id': sync_id,
            'status': status,
            'trigger': trigger,
            'file_name': file_name,
            'file_path': blob_path,
            'records_total': total_rows,
            'records_inserted': records_inserted,
            'records_updated': records_updated,
            'records_failed': records_failed,
            'duration_seconds': round(duration_seconds, 2),
            'duration_minutes': round(duration_minutes, 2),
            'rows_per_minute': round(rows_per_minute, 2),
            'rows_per_second': round(rows_per_second, 2),
            'processing_mode': 'streaming',
        }

        # Print summary
        print('SYNC_SUMMARY ' + json.dumps(result, default=str))

        return result

    except Exception as exc:
        logging.error("Streaming sync %s failed: %s", sync_id, exc, exc_info=True)
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            file_path=blob_path,
            file_size_bytes=blob_size or len(raw_content),
            status='FAILED',
            error_message=str(exc),
        )
        send_alert_email(sync_id, file_name, str(exc))
        raise


def _run_streaming_pipeline_with_checkpoints(
        *,
        file_name: str,
        blob_path: str,
        raw_content: bytes,
        trigger: str,
        blob_size: Optional[int] = None,
) -> dict:
    """
    Execute streaming pipeline with parquet-based checkpointing.

    Two-phase approach:
    1. Convert .dat to parquet chunks (if not already done)
    2. Process parquet chunks (resumable from any point)

    Benefits:
    - Fault-tolerant: Resume from last successful chunk on failure
    - 60x faster resume: Read from parquet instead of re-parsing .dat
    - Observable progress: Track completion per chunk
    """

    sync_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now(timezone.utc)

    logging.info("=" * 80)
    logging.info("🚀 STARTING STREAMING PIPELINE WITH CHECKPOINTS")
    logging.info("=" * 80)
    logging.info(
        "Sync ID: %s | File: %s | Size: %.2f MB | Trigger: %s",
        sync_id,
        file_name,
        (blob_size or len(raw_content)) / 1024 / 1024,
        trigger,
    )

    # Persist initial sync metadata
    update_sync_metadata(
        sync_id=sync_id,
        file_name=file_name,
        file_path=blob_path,
        file_size_bytes=blob_size or len(raw_content),
        status='IN_PROGRESS',
    )

    # Initialize checkpoint manager
    # Azure Functions only allows writes to /tmp directory
    checkpoint_base = os.getenv('CHECKPOINT_BASE_PATH', '/tmp/checkpoints')
    checkpoint_manager = CheckpointManager(sync_id, file_name, checkpoint_base)

    try:
        # PHASE 1: Convert .dat to Parquet Chunks (if not already done)
        # ==============================================================
        if not checkpoint_manager.checkpoint_exists():
            logging.info("📦 PHASE 1: Converting .dat file to parquet chunks...")

            # Determine optimal chunk size
            file_size = blob_size or len(raw_content)
            available_memory_mb = int(os.getenv('MAX_MEMORY_MB', '512'))
            chunk_size = _estimate_chunk_size(file_size, available_memory_mb)

            # Create checkpoint structure
            estimated_rows = file_size // 1000  # Rough estimate: 1KB per row
            estimated_chunks = max(1, estimated_rows // chunk_size)

            parser_type, parser_desc = _get_parser_info()
            logging.info(f"📊 Using {parser_desc} for checkpoint conversion")

            checkpoint_manager.create_checkpoint(
                total_chunks=estimated_chunks,
                chunk_size=chunk_size,
                file_size_bytes=file_size,
                source_path=blob_path
            )

            # Parse .dat file into chunks and save as parquet
            chunk_iterator = _parse_dat_chunks(
                raw_content,
                chunk_size=chunk_size,
                decompress=True
            )

            chunk_id = 0
            for chunk_df in chunk_iterator:
                # Save chunk as parquet (fast columnar format, compressed)
                checkpoint_manager.save_chunk_parquet(chunk_id, chunk_df)
                chunk_id += 1

                logging.info(f"✅ Converted chunk {chunk_id} to parquet ({len(chunk_df)} rows)")

            # Update manifest with actual chunk count
            checkpoint_manager.manifest['total_chunks'] = chunk_id
            checkpoint_manager.finalize_conversion()

            logging.info(f"✅ PHASE 1 COMPLETE: Converted {chunk_id} chunks to parquet")

        else:
            logging.info("♻️  PHASE 1 SKIPPED: Parquet checkpoints already exist (resuming)")

        # PHASE 2: Process Parquet Chunks (Resumable)
        # ============================================
        logging.info("⚙️  PHASE 2: Processing parquet chunks...")

        # Get pending chunks (not yet completed)
        pending_chunks = checkpoint_manager.get_pending_chunks()
        total_chunks = checkpoint_manager.get_total_chunks()
        completed_initial = total_chunks - len(pending_chunks)

        logging.info(f"📊 Chunk status: {completed_initial} completed, {len(pending_chunks)} pending out of {total_chunks} total")
        logging.info(f"📋 Pending chunk IDs: {pending_chunks}")
        sys.stdout.flush()

        if completed_initial > 0:
            logging.info(
                f"♻️  RESUMING from checkpoint: {completed_initial}/{total_chunks} chunks already completed, "
                f"{len(pending_chunks)} remaining"
            )
        else:
            logging.info(f"📊 Processing {total_chunks} chunks from start")

        # Get error threshold from config
        error_threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '1.0'))

        # Create chunked processor
        processor = ChunkedPipelineProcessor(sync_id, file_name, error_threshold)

        # Process all pending chunks
        # With 20K rows/chunk taking ~60-90 seconds each, we should be able to process
        # 5-6 chunks within the 10-minute Azure Functions timeout
        logging.info(f"🔄 Starting chunk processing loop: {len(pending_chunks)} chunks to process")
        sys.stdout.flush()

        chunks_processed = 0
        for chunk_id in pending_chunks:
            try:
                chunk_num = chunk_id + 1
                logging.info(f"📦 Processing chunk {chunk_num}/{total_chunks} (ID: {chunk_id})...")
                sys.stdout.flush()

                # Load chunk from parquet (60x faster than parsing .dat)
                logging.info(f"Loading chunk {chunk_num} from parquet...")
                sys.stdout.flush()
                chunk_df = checkpoint_manager.load_chunk(chunk_id)
                logging.info(f"Loaded chunk {chunk_num}: {len(chunk_df)} rows")
                sys.stdout.flush()

                # Process chunk: validate, transform, upsert
                logging.info(f"Calling processor.process_chunk() for chunk {chunk_num}...")
                sys.stdout.flush()

                try:
                    chunk_inserted, chunk_updated = processor.process_chunk(chunk_df)
                    logging.info(f"✅ processor.process_chunk() returned: +{chunk_inserted} inserted, +{chunk_updated} updated")
                    sys.stdout.flush()
                except Exception as process_error:
                    logging.error(
                        f"❌ FATAL: processor.process_chunk() failed for chunk {chunk_num}: {process_error}",
                        exc_info=True
                    )
                    sys.stdout.flush()
                    sys.stderr.flush()
                    raise  # Re-raise to outer handler

                # Mark chunk as completed in manifest
                checkpoint_manager.mark_chunk_complete(
                    chunk_id,
                    records_inserted=chunk_inserted,
                    records_updated=chunk_updated,
                    validation_errors=0  # Validation errors already tracked in processor
                )

                chunks_processed += 1
                logging.info(
                    f"✅ Chunk {chunk_num}/{total_chunks} complete: "
                    f"+{chunk_inserted} inserted, +{chunk_updated} updated "
                    f"[{chunks_processed}/{len(pending_chunks)} chunks processed so far]"
                )
                sys.stdout.flush()

            except Exception as chunk_error:
                logging.error(f"❌ Chunk {chunk_id} failed: {chunk_error}", exc_info=True)
                sys.stdout.flush()
                sys.stderr.flush()
                checkpoint_manager.mark_chunk_failed(chunk_id, str(chunk_error))
                # Don't continue - stop processing if a chunk fails
                raise

        logging.info(f"🏁 Chunk processing loop completed: {chunks_processed}/{len(pending_chunks)} chunks processed")
        sys.stdout.flush()


        # Get processing summary
        summary = processor.get_summary()
        progress = checkpoint_manager.get_progress_summary()

        # --- NEW: determine validation error types (global vs row-level) ---
        validation_errors = summary.get('validation_errors') or []
        row_level_errors = [e for e in validation_errors if isinstance(e, dict) and 'row' in e]
        global_errors = [e for e in validation_errors if not (isinstance(e, dict) and 'row' in e)]
        failed_rows_count = summary.get('failed_rows', 0)

        # Diagnostic logging for status determination
        logging.info(
            f"📊 Validation summary: {len(validation_errors)} total errors "
            f"({len(global_errors)} global, {len(row_level_errors)} row-level), "
            f"{failed_rows_count} failed rows"
        )

        # Diagnostic logging to identify any discrepancies
        if summary['records_inserted'] != progress['total_records_inserted'] or \
           summary['records_updated'] != progress['total_records_updated']:
            logging.warning(
                "⚠️  Discrepancy detected between processor and checkpoint manager counts: "
                "processor={inserted=%s, updated=%s} vs checkpoint={inserted=%s, updated=%s}. "
                "Using checkpoint manager values (more accurate).",
                summary['records_inserted'],
                summary['records_updated'],
                progress['total_records_inserted'],
                progress['total_records_updated']
            )

        # Refresh materialized views
        try:
            if os.getenv('ENABLE_MV_REFRESH', 'true').lower() == 'true':
                refresh_materialized_views()
                logging.info("Materialized views refreshed")
            else:
                logging.info("Materialized view refresh skipped via config")
        except Exception as mv_error:
            logging.error(f"❌ Materialized view refresh failed: {mv_error}", exc_info=True)
            # Continue to cache prepopulation even if MV refresh fails

        # Trigger cache prepopulation (always runs at end of pipeline)
        if os.getenv('ENABLE_CACHE_PREPOPULATION', 'true').lower() == 'true':
            from helpers.cache_prepopulator import trigger_cache_repopulation_safe
            logging.info("Triggering cache prepopulation...")
            success = trigger_cache_repopulation_safe(
                clear_first=True,
                concurrency=15
            )
            if success:
                logging.info("Cache prepopulation job started successfully")
            else:
                logging.warning("Cache prepopulation trigger failed (non-critical, continuing...)")
        else:
            logging.info("Cache prepopulation skipped via config")

        # --- NEW: Determine final status taking global (bad input) errors into account ---
        error_message = None
        if global_errors and failed_rows_count > 0:
            status = 'PARTIAL_SUCCESS_BAD_INPUT'
            first_global = global_errors[0]
            error_message = first_global.get('message') if isinstance(first_global, dict) else str(first_global)
            logging.warning("Detected global bad-input errors and row-level failures: %s", error_message)
        elif global_errors:
            status = 'BAD_INPUT'
            first_global = global_errors[0]
            error_message = first_global.get('message') if isinstance(first_global, dict) else str(first_global)
            logging.error("Detected fatal global bad-input errors: %s", error_message)
        elif progress['failed_chunks'] > 0:
            status = 'PARTIAL_SUCCESS'
            logging.warning(
                f"Sync partially successful: {progress['failed_chunks']} chunks failed, "
                f"will retry on next run"
            )
        elif summary.get('failed_rows', 0) > 0:
            # Check if failed rows are due to validation errors (bad input)
            if row_level_errors:
                status = 'PARTIAL_SUCCESS_BAD_INPUT'
                logging.warning(
                    f"Sync partially successful with bad input data: {failed_rows_count} validation errors"
                )
            else:
                status = 'PARTIAL_SUCCESS'
        else:
            status = 'SUCCESS'

        # Update sync metadata - use checkpoint manager's tracked values for accuracy
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            file_path=blob_path,
            file_size_bytes=blob_size or len(raw_content),
            status=status,
            records_total=summary['total_rows'],
            records_inserted=progress['total_records_inserted'],
            records_updated=progress['total_records_updated'],
            records_failed=summary.get('failed_rows', 0),
            error_message=error_message,
            validation_errors=validation_errors if validation_errors else None,
        )

        # Send alerts if needed
        if validation_errors or progress['failed_chunks'] > 0:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with {summary.get('failed_rows', 0)} validation errors and "
                f"{progress['failed_chunks']} failed chunks",
                validation_errors,
                is_warning=True,
            )

        # Cleanup checkpoint files on complete success
        cleanup_on_success = os.getenv('CHECKPOINT_CLEANUP_ON_SUCCESS', 'true').lower() == 'true'
        if status == 'SUCCESS' and cleanup_on_success:
            checkpoint_manager.cleanup()
            logging.info("🗑️  Checkpoint files cleaned up after successful sync")
        else:
            logging.info(
                f"♻️  Checkpoint files retained for retry "
                f"(status={status}, cleanup_on_success={cleanup_on_success})"
            )

        # Calculate performance metrics
        end_time = datetime.now(timezone.utc)
        duration_seconds = (end_time - start_time).total_seconds()
        duration_minutes = duration_seconds / 60
        total_rows = summary['total_rows']
        rows_per_minute = total_rows / duration_minutes if duration_minutes > 0 else 0
        rows_per_second = total_rows / duration_seconds if duration_seconds > 0 else 0

        # Use checkpoint manager's tracked values for accurate reporting
        records_inserted = progress['total_records_inserted']
        records_updated = progress['total_records_updated']

        logging.info(
            "Checkpointed streaming sync %s completed (%s): "
            "inserted=%s updated=%s errors=%s | "
            "Performance: %s rows in %.2f min (%.0f rows/min, %.1f rows/sec) | "
            "Chunks: %s total, %s completed, %s failed",
            sync_id,
            status,
            records_inserted,
            records_updated,
            summary.get('failed_rows', 0),
            total_rows,
            duration_minutes,
            rows_per_minute,
            rows_per_second,
            progress['total_chunks'],
            progress['completed_chunks'],
            progress['failed_chunks'],
        )

        result = {
            'sync_id': sync_id,
            'status': status,
            'trigger': trigger,
            'file_name': file_name,
            'file_path': blob_path,
            'records_total': total_rows,
            'records_inserted': records_inserted,
            'records_updated': records_updated,
            'records_failed': summary.get('failed_rows', 0),
            'duration_seconds': round(duration_seconds, 2),
            'duration_minutes': round(duration_minutes, 2),
            'rows_per_minute': round(rows_per_minute, 2),
            'rows_per_second': round(rows_per_second, 2),
            'processing_mode': 'streaming_with_checkpoints',
            'checkpoint_stats': {
                'total_chunks': progress['total_chunks'],
                'completed_chunks': progress['completed_chunks'],
                'failed_chunks': progress['failed_chunks'],
                'resumed_from_checkpoint': completed_initial > 0,
                'chunks_skipped': completed_initial,
            }
        }

        # Print summary
        print('SYNC_SUMMARY ' + json.dumps(result, default=str))

        return result

    except Exception as exc:
        logging.error("Checkpointed streaming sync %s failed: %s", sync_id, exc, exc_info=True)
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            file_path=blob_path,
            file_size_bytes=blob_size or len(raw_content),
            status='FAILED',
            error_message=str(exc),
        )
        send_alert_email(sync_id, file_name, str(exc))
        raise


@app.blob_trigger(
    arg_name="myblob",
    path="data/hex-ztdwr/{name}",
    connection="AzureWebJobsStorage",
)
def ztdwr_sync(myblob: func.InputStream):
    """Automatically triggered when a new ZTDWR file lands in Azure Storage."""

    import sys

    # CRITICAL: Force immediate log flushing - log before ANY blocking operations
    logging.info("=" * 80)
    logging.info("🚀 FUNCTION STARTED - Blob trigger received")
    logging.info("=" * 80)
    sys.stdout.flush()
    sys.stderr.flush()

    # Defensive check: Ensure myblob is not None
    if myblob is None:
        error_msg = "Blob parameter is None - blob may have been deleted or binding failed"
        logging.error(error_msg)
        sys.stdout.flush()
        raise ValueError(error_msg)

    # Defensive check: Ensure blob has required attributes
    try:
        blob_name = myblob.name
        blob_length = myblob.length
    except AttributeError as e:
        error_msg = f"Blob object missing required attributes: {e}"
        logging.error(error_msg)
        sys.stdout.flush()
        raise ValueError(error_msg)

    # Validate blob name exists
    if not blob_name:
        error_msg = "Blob name is empty or null"
        logging.error(error_msg)
        sys.stdout.flush()
        raise ValueError(error_msg)

    file_name = blob_name.split('/')[-1]
    file_size_mb = (blob_length or 0) / 1024 / 1024

    logging.info("📋 Blob: %s | Size: %s bytes (%.2f MB)",
                 file_name, blob_length, file_size_mb)
    sys.stdout.flush()

    # Download blob content with progress logging for large files
    logging.info("📥 Starting blob download... (%.2f MB)", file_size_mb)
    sys.stdout.flush()

    download_start = datetime.now(timezone.utc)
    raw_content = None

    try:
        # For large files, read in chunks with progress logging
        if file_size_mb > 10:
            logging.info("⚠️  Large file detected - using chunked download with progress tracking")
            sys.stdout.flush()

            chunks = []
            bytes_downloaded = 0
            chunk_size = 10 * 1024 * 1024  # 10MB chunks
            last_log_time = datetime.now(timezone.utc)

            while True:
                chunk = myblob.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_downloaded += len(chunk)

                # Log progress every 10 seconds or 50MB to show function is alive
                current_time = datetime.now(timezone.utc)
                if (current_time - last_log_time).total_seconds() > 10 or bytes_downloaded % (50 * 1024 * 1024) < chunk_size:
                    pct = (bytes_downloaded / blob_length * 100) if blob_length else 0
                    elapsed = (current_time - download_start).total_seconds()
                    speed_mbps = (bytes_downloaded / 1024 / 1024) / elapsed if elapsed > 0 else 0
                    logging.info("⏳ Download progress: %.1f%% (%d/%d MB) at %.2f MB/s",
                                pct, bytes_downloaded // (1024*1024),
                                (blob_length or 0) // (1024*1024), speed_mbps)
                    sys.stdout.flush()
                    last_log_time = current_time

            raw_content = b''.join(chunks)
            logging.info("✅ Chunked download complete: %d bytes", len(raw_content))
            sys.stdout.flush()
        else:
            # Small files - direct read
            raw_content = myblob.read()
            logging.info("✅ Direct download complete: %d bytes", len(raw_content))
            sys.stdout.flush()

    except Exception as e:
        error_msg = f"Failed to download blob content: {e}"
        logging.error("❌ %s", error_msg, exc_info=True)
        sys.stdout.flush()
        raise RuntimeError(error_msg) from e

    # Validate we got content
    if raw_content is None or len(raw_content) == 0:
        error_msg = f"Blob download returned empty content (blob_name={file_name}, expected_size={blob_length})"
        logging.error(error_msg)
        sys.stdout.flush()
        raise ValueError(error_msg)

    download_duration = (datetime.now(timezone.utc) - download_start).total_seconds()
    download_speed = (len(raw_content) / 1024 / 1024) / download_duration if download_duration > 0 else 0
    logging.info("📊 Download summary: %s bytes in %.2f seconds (%.2f MB/s)",
                 len(raw_content), download_duration, download_speed)
    sys.stdout.flush()

    # Determine which pipeline to use based on file size
    file_size = myblob.length or len(raw_content)
    file_size_mb = file_size / 1024 / 1024

    # Get thresholds from config
    streaming_threshold_mb = int(os.getenv('STREAMING_THRESHOLD_MB', '20'))
    checkpoint_threshold_mb = int(os.getenv('CHECKPOINT_THRESHOLD_MB', '50'))
    use_streaming = os.getenv('ENABLE_STREAMING', 'true').lower() == 'true'
    use_checkpointing = os.getenv('ENABLE_CHECKPOINTING', 'true').lower() == 'true'

    logging.info(
        "📊 PIPELINE SELECTION: file_size=%.2f MB, streaming_threshold=%d MB, "
        "checkpoint_threshold=%d MB, streaming_enabled=%s, checkpointing_enabled=%s",
        file_size_mb, streaming_threshold_mb, checkpoint_threshold_mb,
        use_streaming, use_checkpointing
    )

    # Pipeline selection logic:
    # 1. Very large files (>50MB) → Streaming with checkpoints (most robust)
    # 2. Large files (>20MB) → Streaming without checkpoints
    # 3. Small files (<=20MB) → Standard pipeline

    if use_checkpointing and use_streaming and file_size_mb > checkpoint_threshold_mb:
        logging.info(
            "🔄 Selected: STREAMING PIPELINE WITH CHECKPOINTS "
            "(file size %.2f MB > %d MB checkpoint threshold)",
            file_size_mb, checkpoint_threshold_mb
        )
        _run_streaming_pipeline_with_checkpoints(
            file_name=file_name,
            blob_path=myblob.name,
            raw_content=raw_content,
            trigger='blob',
            blob_size=myblob.length,
        )
    elif use_streaming and file_size_mb > streaming_threshold_mb:
        logging.info(
            "🌊 Selected: STREAMING PIPELINE (file size %.2f MB > %d MB threshold)",
            file_size_mb, streaming_threshold_mb
        )
        _run_streaming_pipeline(
            file_name=file_name,
            blob_path=myblob.name,
            raw_content=raw_content,
            trigger='blob',
            blob_size=myblob.length,
        )
    else:
        if not use_streaming:
            logging.info(
                "📄 Selected: STANDARD PIPELINE (streaming disabled via config)"
            )
        else:
            logging.info(
                "📄 Selected: STANDARD PIPELINE (file size %.2f MB <= %d MB threshold)",
                file_size_mb, streaming_threshold_mb
            )
        _run_sync_pipeline(
            file_name=file_name,
            blob_path=myblob.name,
            raw_content=raw_content,
            trigger='blob',
            blob_size=myblob.length,
        )


@app.route(route="healthz", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def healthz(req: func.HttpRequest) -> func.HttpResponse:
    """Simple health check endpoint for uptime probes."""

    # Use logging (no debug print)
    logging.info('Health check requested')

    try:
        conn = get_postgres_connection()
        conn.close()
        db_status = "healthy"
        logging.info('Database connection check: %s', db_status)
    except Exception as exc:
        logging.error("Database health check failed: %s", exc)
        db_status = f"unhealthy: {exc}"

    return func.HttpResponse(
        body=json.dumps({"status": "ok", "database": db_status, "timestamp": datetime.now(timezone.utc).isoformat()}),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="sync-ztdwr", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def manual_sync(req: func.HttpRequest) -> func.HttpResponse:
    """Trigger the sync pipeline via HTTP for a specific blob."""

    try:
        payload = req.get_json()
    except ValueError:
        payload = {}

    blob_identifier = (
            payload.get('blob_path')
            or payload.get('blob_name')
            or payload.get('file_name')
            or req.params.get('blob_path')
            or req.params.get('blob_name')
            or req.params.get('file_name')
    )

    if not blob_identifier:
        return func.HttpResponse(
            json.dumps({'error': 'blob_name or blob_path is required'}),
            status_code=400,
            mimetype='application/json',
        )

    container = (
            payload.get('container')
            or req.params.get('container')
            or os.getenv('MANUAL_SYNC_CONTAINER', 'data')
    )

    if '/' in blob_identifier:
        first, rest = blob_identifier.split('/', 1)
        if first == container:
            blob_path = rest
        else:
            blob_path = blob_identifier
    else:
        blob_path = blob_identifier

    if not blob_path.startswith('hex-ztdwr/'):
        blob_path = f'hex-ztdwr/{blob_path}'

    storage_conn = os.getenv('AzureWebJobsStorage')
    if not storage_conn:
        return func.HttpResponse(
            json.dumps({'error': 'AzureWebJobsStorage connection string is not configured'}),
            status_code=500,
            mimetype='application/json',
        )

    try:
        blob_service = BlobServiceClient.from_connection_string(
            storage_conn,
            logging_enable=False  # Disable HTTP request/response logging
        )
        container_client = blob_service.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        downloader = blob_client.download_blob()
        raw_content = downloader.readall()
        blob_size = downloader.size or len(raw_content)
        file_name = os.path.basename(blob_path)

        # Determine which pipeline to use based on file size
        file_size = blob_size or len(raw_content)
        file_size_mb = file_size / 1024 / 1024

        # Get thresholds from config
        streaming_threshold_mb = int(os.getenv('STREAMING_THRESHOLD_MB', '20'))
        checkpoint_threshold_mb = int(os.getenv('CHECKPOINT_THRESHOLD_MB', '50'))
        use_streaming = os.getenv('ENABLE_STREAMING', 'true').lower() == 'true'
        use_checkpointing = os.getenv('ENABLE_CHECKPOINTING', 'true').lower() == 'true'

        # Pipeline selection (same logic as blob trigger)
        if use_checkpointing and use_streaming and file_size_mb > checkpoint_threshold_mb:
            logging.info(
                f"Manual sync: File size {file_size_mb:.2f} MB > {checkpoint_threshold_mb} MB, "
                f"using STREAMING pipeline WITH CHECKPOINTS"
            )
            result = _run_streaming_pipeline_with_checkpoints(
                file_name=file_name,
                blob_path=f"{container}/{blob_path}",
                raw_content=raw_content,
                trigger='http',
                blob_size=blob_size,
            )
        elif use_streaming and file_size_mb > streaming_threshold_mb:
            logging.info(
                f"Manual sync: File size {file_size_mb:.2f} MB > {streaming_threshold_mb} MB threshold, "
                f"using STREAMING pipeline"
            )
            result = _run_streaming_pipeline(
                file_name=file_name,
                blob_path=f"{container}/{blob_path}",
                raw_content=raw_content,
                trigger='http',
                blob_size=blob_size,
            )
        else:
            logging.info(
                f"Manual sync: File size {file_size_mb:.2f} MB <= {streaming_threshold_mb} MB threshold, "
                f"using STANDARD pipeline"
            )
            result = _run_sync_pipeline(
                file_name=file_name,
                blob_path=f"{container}/{blob_path}",
                raw_content=raw_content,
                trigger='http',
                blob_size=blob_size,
            )

        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype='application/json',
        )

    except ResourceNotFoundError:
        message = f"Blob {container}/{blob_path} not found"
        logging.error(message)
        return func.HttpResponse(
            json.dumps({'error': message}),
            status_code=404,
            mimetype='application/json',
        )

    except Exception as exc:  # Broad catch to ensure HTTP response is returned
        logging.error(
            "Manual sync failed for %s/%s: %s",
            container,
            blob_path,
            exc,
            exc_info=True,
        )
        return func.HttpResponse(
            json.dumps({'error': str(exc)}),
            status_code=500,
            mimetype='application/json',
        )

