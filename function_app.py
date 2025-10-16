"""
Azure Function for SAP ZTDWR Data Sync
Triggered by blob upload to hex-ztdwr/ container
"""

import azure.functions as func
import logging
import gzip
import io
import os
from datetime import datetime
import uuid

# Import helper modules
from helpers.parser import parse_ztdwr_file, is_gzipped
from helpers.validator import validate_ztdwr_data
from helpers.transformer import transform_to_transport_documents
from helpers.postgres_client import (
    get_postgres_connection,
    upsert_to_postgres,
    refresh_materialized_views,
    update_sync_metadata,
)
from helpers.notifier import send_alert_email

app = func.FunctionApp()

@app.blob_trigger(
    arg_name="myblob",
    path="data/hex-ztdwr/{name}",
    connection="AzureWebJobsStorage"
)
def ztdwr_sync(myblob: func.InputStream):
    """
    Triggered when new ZTDWR .dat file is uploaded to Azure Storage.

    Process:
    1. Download blob
    2. Decompress gzip (if applicable)
    3. Parse CSV/TSV
    4. Validate data
    5. Transform to schema
    6. Upsert to PostgreSQL
    7. Refresh materialized views
    8. Update sync metadata
    """

    logging.info(f"Python blob trigger function processing blob")
    logging.info(f"Name: {myblob.name}")
    logging.info(f"Blob Size: {myblob.length} bytes")

    # Generate unique sync ID
    sync_id = f"sync_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    file_name = myblob.name.split('/')[-1]

    # Initialize metadata
    update_sync_metadata(
        sync_id=sync_id,
        file_name=file_name,
        file_path=myblob.name,
        file_size_bytes=myblob.length,
        status='IN_PROGRESS'
    )

    try:
        # Step 1: Read blob content
        blob_content = myblob.read()
        logging.info(f"Downloaded {len(blob_content)} bytes")

        # Step 2: Decompress (if gzipped)
        if is_gzipped(blob_content):
            decompressed = gzip.decompress(blob_content)
            logging.info(f"Decompressed to {len(decompressed)} bytes")
        else:
            decompressed = blob_content
            logging.info("File is not gzipped, using as-is")

        # Step 3: Parse CSV/TSV
        df = parse_ztdwr_file(decompressed)
        logging.info(f"Parsed {len(df)} records")

        # Step 4: Validate data
        validation_errors = validate_ztdwr_data(df)

        if validation_errors:
            error_count = len(validation_errors)
            error_rate = error_count / len(df) if len(df) > 0 else 0
            logging.warning(f"Found {error_count} validation errors ({error_rate:.2%})")

            # Check if error rate exceeds threshold
            threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '0.10'))
            if error_rate > threshold:
                error_msg = (
                    f"Validation error rate {error_rate:.2%} exceeds "
                    f"threshold {threshold:.2%}. Rejecting entire file."
                )
                logging.error(error_msg)

                update_sync_metadata(
                    sync_id=sync_id,
                    file_name=file_name,
                    status='FAILED',
                    records_total=len(df),
                    records_failed=error_count,
                    error_message=error_msg,
                    validation_errors=validation_errors
                )

                send_alert_email(sync_id, file_name, error_msg, validation_errors)
                raise ValueError(error_msg)

            # Process valid rows only
            invalid_indices = [err['row'] for err in validation_errors if 'row' in err]
            df = df.drop(index=invalid_indices)
            logging.info(f"Processing {len(df)} valid records after filtering")

        # Step 5: Transform to schema
        transformed_df = transform_to_transport_documents(df, sync_id, file_name)
        logging.info("Data transformed to transport_documents schema")

        # Step 6: Upsert to PostgreSQL
        records_inserted, records_updated = upsert_to_postgres(
            sync_id,
            transformed_df
        )
        logging.info(
            f"Upsert completed: {records_inserted} inserted, {records_updated} updated"
        )

        # Step 7: Refresh materialized views
        if os.getenv('ENABLE_MV_REFRESH', 'true').lower() == 'true':
            refresh_materialized_views()
            logging.info("Materialized views refreshed")
        else:
            logging.info("MV refresh disabled via config")

        # Step 8: Update sync metadata with success
        status = 'PARTIAL_SUCCESS' if validation_errors else 'SUCCESS'
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            status=status,
            records_total=len(df) + len(validation_errors) if validation_errors else len(df),
            records_inserted=records_inserted,
            records_updated=records_updated,
            records_failed=len(validation_errors) if validation_errors else 0,
            validation_errors=validation_errors if validation_errors else None
        )

        logging.info(f"Sync {sync_id} completed with status: {status}")

        if validation_errors:
            send_alert_email(
                sync_id,
                file_name,
                f"Sync completed with {len(validation_errors)} validation errors",
                validation_errors,
                is_warning=True
            )

    except Exception as e:
        logging.error(f"Sync {sync_id} failed: {str(e)}", exc_info=True)

        # Update metadata with failure
        update_sync_metadata(
            sync_id=sync_id,
            file_name=file_name,
            status='FAILED',
            error_message=str(e)
        )

        # Send alert
        send_alert_email(sync_id, file_name, str(e))

        # Re-raise to trigger Azure Function retry
        raise


@app.route(route="healthz", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def healthz(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    logging.info('Health check requested')

    # Check database connectivity
    try:
        conn = get_postgres_connection()
        conn.close()
        db_status = "healthy"
    except Exception as e:
        logging.error(f"Database health check failed: {str(e)}")
        db_status = f"unhealthy: {str(e)}"

    return func.HttpResponse(
        body=f'{{"status": "ok", "database": "{db_status}"}}',
        status_code=200,
        mimetype="application/json"
    )
