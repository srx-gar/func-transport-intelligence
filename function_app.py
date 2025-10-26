"""Azure Functions entrypoints for the transport intelligence ETL."""

import gzip
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional

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

app = func.FunctionApp()


def _run_sync_pipeline(
        *,
        file_name: str,
        blob_path: str,
        raw_content: bytes,
        trigger: str,
        blob_size: Optional[int] = None,
) -> dict:
    """Execute the end-to-end sync pipeline and return a summary result."""

    sync_id = f"sync_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logging.info(
        "Starting sync %s (trigger=%s, file=%s, path=%s, size=%s)",
        sync_id,
        trigger,
        file_name,
        blob_path,
        blob_size,
    )

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

        # Step 2: Parse raw text into a DataFrame
        df = parse_ztdwr_file(content)
        total_rows = len(df)
        logging.info("Parsed %s rows from %s", total_rows, file_name)

        # Step 3: Validate source data
        validation_errors = validate_ztdwr_data(df)
        error_count = len(validation_errors)
        error_rate = error_count / total_rows if total_rows else 0

        if error_count:
            logging.warning(
                "Validation detected %s issues (rate %.2f%%)",
                error_count,
                error_rate * 100,
            )

            threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '0.10'))
            if error_rate > threshold:
                error_msg = (
                    f"Validation error rate {error_rate:.2%} exceeds threshold {threshold:.2%}."
                )
                update_sync_metadata(
                    sync_id=sync_id,
                    file_name=file_name,
                    status='FAILED',
                    records_total=total_rows,
                    records_failed=error_count,
                    error_message=error_msg,
                    validation_errors=validation_errors,
                )
                send_alert_email(sync_id, file_name, error_msg, validation_errors)
                raise ValueError(error_msg)

            invalid_indices = [err['row'] for err in validation_errors if isinstance(err, dict) and 'row' in err]
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

        status = 'PARTIAL_SUCCESS' if validation_errors else 'SUCCESS'
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            status=status,
            records_total=total_rows,
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_failed=error_count,
            validation_errors=validation_errors if validation_errors else None,
        )

        if validation_errors:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with {error_count} validation errors",
                validation_errors,
                is_warning=True,
            )

        logging.info(
            "Sync %s completed (%s): inserted=%s updated=%s errors=%s",
            sync_id,
            status,
            records_inserted,
            records_updated,
            error_count,
        )

        return {
            'sync_id': sync_id,
            'status': status,
            'trigger': trigger,
            'file_name': file_name,
            'file_path': blob_path,
            'records_total': total_rows,
            'records_inserted': records_inserted,
            'records_updated': records_updated,
            'records_failed': error_count,
        }

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
        body=json.dumps({"status": "ok", "database": db_status}),
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
