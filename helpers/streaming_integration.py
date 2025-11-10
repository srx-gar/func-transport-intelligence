"""
Example integration of streaming/chunked processing into function_app.py

This shows how to use the optimized processing for large files while
maintaining backward compatibility with the existing approach.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from helpers.streaming_parser import parse_ztdwr_chunks, estimate_chunk_size
from helpers.chunked_processor import ChunkedPipelineProcessor, process_chunks_with_backpressure
from helpers.postgres_client import (
    refresh_materialized_views,
    update_sync_metadata,
)
from helpers.notifier import send_alert_email


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

    Memory-efficient approach that processes files in chunks.
    """

    sync_id = f"sync_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logging.info(
        "Starting STREAMING sync %s (trigger=%s, file=%s, path=%s, size=%s)",
        sync_id,
        trigger,
        file_name,
        blob_path,
        blob_size,
    )

    # Persist initial sync metadata
    update_sync_metadata(
        sync_id=sync_id,
        file_name=file_name,
        file_path=blob_path,
        file_size_bytes=blob_size or len(raw_content),
        status='IN_PROGRESS',
    )

    global_validation_error = None
    try:
        # Determine optimal chunk size based on file size
        file_size = blob_size or len(raw_content)
        available_memory_mb = int(os.getenv('MAX_MEMORY_MB', '512'))
        chunk_size = estimate_chunk_size(file_size, available_memory_mb)

        logging.info(f"Using chunk size: {chunk_size} rows")

        # Get error threshold from config
        error_threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '0.05'))

        # Create chunked processor
        processor = ChunkedPipelineProcessor(sync_id, file_name, error_threshold)

        # Create chunk iterator
        chunk_iterator = parse_ztdwr_chunks(
            raw_content,
            chunk_size=chunk_size,
            decompress=True  # Will auto-detect and decompress if needed
        )

        # Process chunks with backpressure control
        max_buffer = int(os.getenv('MAX_BUFFER_CHUNKS', '2'))
        try:
            summary = process_chunks_with_backpressure(
                chunk_iterator,
                processor,
                max_buffer_chunks=max_buffer
            )
        except ValueError as ve:
            # This is likely a global validation error (bad input data)
            logging.error(f"Global validation error: {ve}")
            global_validation_error = str(ve)
            # We'll check for row-level errors after this block
            summary = {
                'total_rows': 0,
                'records_inserted': 0,
                'records_updated': 0,
                'failed_rows': 0,
                'validation_errors': [],
            }

        # Check for processing errors
        if 'error' in summary:
            raise RuntimeError(summary['error'])

        # Extract results
        total_rows = summary['total_rows']
        records_inserted = summary['records_inserted']
        records_updated = summary['records_updated']
        records_failed = summary['failed_rows']
        validation_errors = summary['validation_errors']

        # Refresh materialized views
        if os.getenv('ENABLE_MV_REFRESH', 'true').lower() == 'true':
            refresh_materialized_views()
            logging.info("Materialized views refreshed")

            # Trigger cache prepopulation after MV refresh
            if os.getenv('ENABLE_CACHE_PREPOPULATION', 'true').lower() == 'true':
                from helpers.cache_prepopulator import trigger_cache_prepopulation_safe
                logging.info("Triggering cache prepopulation...")
                success = trigger_cache_prepopulation_safe(
                    clear_first=True,
                    concurrency=15
                )
                if success:
                    logging.info("Cache prepopulation job started successfully")
                else:
                    logging.warning("Cache prepopulation trigger failed (non-critical, continuing...)")
            else:
                logging.info("Cache prepopulation skipped via config")
        else:
            logging.info("Materialized view refresh skipped via config")

        # Determine status
        if global_validation_error and records_failed:
            status = 'PARTIAL_SUCCESS_BAD_INPUT'
        elif global_validation_error:
            status = 'BAD_INPUT'
        elif records_failed:
            status = 'PARTIAL_SUCCESS'
        else:
            status = 'SUCCESS'

        # Update final metadata
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            status=status,
            records_total=total_rows,
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_failed=records_failed,
            error_message=global_validation_error if global_validation_error else None,
            validation_errors=validation_errors if validation_errors else None,
        )

        # Send alerts if needed
        if global_validation_error:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with bad input data: {global_validation_error}",
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

        logging.info(
            "Streaming sync %s completed (%s): inserted=%s updated=%s errors=%s",
            sync_id,
            status,
            records_inserted,
            records_updated,
            records_failed,
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
            'processing_mode': 'streaming',
        }

        # Print summary
        print('SYNC_SUMMARY ' + json.dumps(result, default=str))

        return result

    except Exception as exc:
        logging.error("Streaming sync %s failed: %s", sync_id, exc, exc_info=True)
        # Only update status to FAILED if not already BAD_INPUT
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            status='FAILED',
            error_message=str(exc),
        )
        send_alert_email(sync_id, file_name, str(exc))
        raise


def should_use_streaming(blob_size: Optional[int], raw_content: bytes = None) -> bool:
    """
    Determine if streaming pipeline should be used based on file size.

    Args:
        blob_size: Size of blob in bytes
        raw_content: Raw content (if blob_size not available)

    Returns:
        True if streaming should be used, False otherwise
    """
    # Check if streaming is enabled
    if os.getenv('ENABLE_STREAMING', 'true').lower() != 'true':
        return False

    # Get file size
    file_size = blob_size
    if file_size is None and raw_content:
        file_size = len(raw_content)

    if file_size is None:
        logging.warning("Cannot determine file size, using standard pipeline")
        return False

    # Get threshold from config (default 50MB)
    threshold_mb = int(os.getenv('STREAMING_THRESHOLD_MB', '50'))
    threshold_bytes = threshold_mb * 1024 * 1024

    use_streaming = file_size > threshold_bytes

    logging.info(
        f"File size: {file_size / 1024 / 1024:.2f} MB, "
        f"threshold: {threshold_mb} MB, "
        f"using {'STREAMING' if use_streaming else 'STANDARD'} pipeline"
    )

    return use_streaming


# Example usage in function_app.py:
#
# @app.blob_trigger(...)
# def ztdwr_sync(myblob: func.InputStream):
#     """Automatically triggered when a new ZTDWR file lands in Azure Storage."""
#
#     logging.info("Blob trigger invoked for %s (%s bytes)", myblob.name, myblob.length)
#     file_name = myblob.name.split('/')[-1]
#     raw_content = myblob.read()
#
#     # Decide which pipeline to use
#     if should_use_streaming(myblob.length, raw_content):
#         _run_streaming_pipeline(
#             file_name=file_name,
#             blob_path=myblob.name,
#             raw_content=raw_content,
#             trigger='blob',
#             blob_size=myblob.length,
#         )
#     else:
#         _run_sync_pipeline(  # Original implementation
#             file_name=file_name,
#             blob_path=myblob.name,
#             raw_content=raw_content,
#             trigger='blob',
#             blob_size=myblob.length,
#         )
