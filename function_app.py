"""Azure Functions entrypoints for the transport intelligence ETL."""

import gzip
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional
import sys

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

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

app = func.FunctionApp()
# Ensure us and local host produce debug logs during development
logging.basicConfig()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
handler.setFormatter(formatter)
root_logger = logging.getLogger()
if not root_logger.handlers:
    root_logger.addHandler(handler)

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
    # Quiet noisy Azure SDK / HTTP logs during normal runs
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.getLogger('azure.functions_worker').setLevel(logging.WARNING)
    logging.getLogger('azure.functions').setLevel(logging.WARNING)
    # Also reduce urllib3/requests noise (HTTP client traces)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

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
    logging.info(
        "Starting sync %s (trigger=%s, file=%s, path=%s, size=%s)",
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

    try:
        # Step 1: Decompress if the payload is gzipped
        if is_gzipped(raw_content):
            content = gzip.decompress(raw_content)
            logging.info(
                "Decompressed payload from %s bytes to %s bytes",
                len(raw_content),
                len(content),
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
                # text str
                parts = content.split('\n', 1)
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
                content = new_header + '\n' + rest
                logging.info("Sanitized header (preview): %s", new_header[:200])
        except Exception:
            logging.exception('Failed to sanitize header; continuing with original content')

        # Step 2: Parse raw text into a DataFrame
        df = parse_ztdwr_file(content)
        total_rows = len(df)
        logging.info("Parsed %s rows from %s", total_rows, file_name)

        # Step 3: Validate source data
        validation_errors = validate_ztdwr_data(df)

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
            # Include global error message and a short dump of errors for diagnostics
            first_msg = None
            try:
                first_msg = global_errors[0].get('message', str(global_errors[0])) if isinstance(global_errors[0], dict) else str(global_errors[0])
            except Exception:
                first_msg = str(global_errors[0])

            error_msg = (
                f"Validation failed due to global errors: {first_msg} (global_errors_count={len(global_errors)}, total_rows={total_rows})"
            )
            logging.error("Validation stopped: %s; global_errors=%s", error_msg, global_errors[:5])
            update_sync_metadata(
                sync_id=sync_id,
                file_name=file_name,
                status='FAILED',
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

            threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '0.10'))
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
        transformed_df = transform_to_transport_documents(df, sync_id, file_name)
        logging.info("Transformation produced %s columns", len(transformed_df.columns))

        # Step 5: Load into PostgreSQL
        records_inserted, records_updated = upsert_to_postgres(sync_id, transformed_df)

        # Step 6: Refresh dependent materialized views
        if os.getenv('ENABLE_MV_REFRESH', 'true').lower() == 'true':
            refresh_materialized_views()
            logging.info("Materialized views refreshed")
        else:
            logging.info("Materialized view refresh skipped via config")

        # Finalize status and record counts
        records_failed = failed_rows_count
        status = 'PARTIAL_SUCCESS' if records_failed else 'SUCCESS'
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
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

        logging.info(
            "Sync %s completed (%s): inserted=%s updated=%s errors=%s",
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
    logging.info(
        "Starting STREAMING sync %s (trigger=%s, file=%s, path=%s, size=%s MB)",
        sync_id,
        trigger,
        file_name,
        blob_path,
        (blob_size or len(raw_content)) / 1024 / 1024,
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
        chunk_size = estimate_chunk_size(file_size, available_memory_mb)

        logging.info(f"Using chunk size: {chunk_size} rows for streaming processing")

        # Get error threshold from config
        error_threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '0.05'))

        # Create chunked processor
        processor = ChunkedPipelineProcessor(sync_id, file_name, error_threshold)

        # Create chunk iterator - will decompress automatically if needed
        chunk_iterator = parse_ztdwr_chunks(
            raw_content,
            chunk_size=chunk_size,
            decompress=True
        )

        # Process chunks with backpressure control
        max_buffer = int(os.getenv('MAX_BUFFER_CHUNKS', '2'))
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

        # Refresh materialized views
        if os.getenv('ENABLE_MV_REFRESH', 'true').lower() == 'true':
            refresh_materialized_views()
            logging.info("Materialized views refreshed")

        # Determine status
        status = 'PARTIAL_SUCCESS' if records_failed else 'SUCCESS'

        # Update final metadata
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            status=status,
            records_total=total_rows,
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_failed=records_failed,
            validation_errors=validation_errors if validation_errors else None,
        )

        # Send alerts if needed
        if validation_errors:
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
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
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

    logging.info("Blob trigger invoked for %s (%s bytes)", myblob.name, myblob.length)
    file_name = myblob.name.split('/')[-1]
    raw_content = myblob.read()

    # Determine which pipeline to use based on file size
    file_size = myblob.length or len(raw_content)
    file_size_mb = file_size / 1024 / 1024

    # Get threshold from config (default 20MB - conservative for production)
    threshold_mb = int(os.getenv('STREAMING_THRESHOLD_MB', '20'))
    use_streaming = os.getenv('ENABLE_STREAMING', 'true').lower() == 'true'

    if use_streaming and file_size_mb > threshold_mb:
        logging.info(
            f"File size {file_size_mb:.2f} MB > {threshold_mb} MB threshold, using STREAMING pipeline"
        )
        _run_streaming_pipeline(
            file_name=file_name,
            blob_path=myblob.name,
            raw_content=raw_content,
            trigger='blob',
            blob_size=myblob.length,
        )
    else:
        logging.info(
            f"File size {file_size_mb:.2f} MB <= {threshold_mb} MB threshold, using STANDARD pipeline"
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

    logging.info('Health check requested')

    try:
        conn = get_postgres_connection()
        conn.close()
        db_status = "healthy"
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
        blob_service = BlobServiceClient.from_connection_string(storage_conn)
        container_client = blob_service.get_container_client(container)
        blob_client = container_client.get_blob_client(blob_path)
        downloader = blob_client.download_blob()
        raw_content = downloader.readall()
        blob_size = downloader.size or len(raw_content)
        file_name = os.path.basename(blob_path)

        # Determine which pipeline to use based on file size
        file_size = blob_size or len(raw_content)
        file_size_mb = file_size / 1024 / 1024

        # Get threshold from config (default 20MB - conservative for production)
        threshold_mb = int(os.getenv('STREAMING_THRESHOLD_MB', '20'))
        use_streaming = os.getenv('ENABLE_STREAMING', 'true').lower() == 'true'

        if use_streaming and file_size_mb > threshold_mb:
            logging.info(
                f"Manual sync: File size {file_size_mb:.2f} MB > {threshold_mb} MB threshold, using STREAMING pipeline"
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
                f"Manual sync: File size {file_size_mb:.2f} MB <= {threshold_mb} MB threshold, using STANDARD pipeline"
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
