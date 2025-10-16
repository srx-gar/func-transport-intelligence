"""
PostgreSQL database operations
"""

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import os
import logging
import json
from datetime import datetime


def get_postgres_connection():
    """
    Get PostgreSQL connection using environment variables.
    """
    # Support both connection string and individual parameters
    conn_str = os.getenv('POSTGRES_CONNECTION_STRING')

    if conn_str:
        conn = psycopg2.connect(conn_str)
    else:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT', '5432'),
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            sslmode='require'
        )

    return conn


def upsert_to_postgres(sync_id: str, df: pd.DataFrame) -> tuple:
    """
    Upsert DataFrame to PostgreSQL using INSERT ... ON CONFLICT.

    Returns (records_inserted, records_updated)
    """
    if len(df) == 0:
        logging.warning("Empty DataFrame, skipping upsert")
        return (0, 0)

    conn = get_postgres_connection()
    cursor = conn.cursor()

    records_inserted = 0
    records_updated = 0

    try:
        # Get column names (excluding index)
        columns = df.columns.tolist()

        # Convert DataFrame to list of tuples
        # Replace NaT and NaN with None for PostgreSQL NULL
        df_clean = df.where(pd.notnull(df), None)
        values = [tuple(row) for row in df_clean.to_numpy()]

        logging.info(f"Upserting {len(values)} records to transport_documents")

        # Build column list (excluding spb_id from update)
        update_cols = [col for col in columns if col not in ['spb_id', 'created_at']]

        # Build INSERT ... ON CONFLICT query
        insert_query = f"""
            INSERT INTO transport_documents ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (spb_id)
            DO UPDATE SET
                {', '.join([f'{col} = EXCLUDED.{col}' for col in update_cols])},
                updated_at = EXCLUDED.updated_at
            RETURNING (xmax = 0) AS inserted
        """

        # Execute batch insert with pagination
        batch_size = 1000
        for i in range(0, len(values), batch_size):
            batch = values[i:i+batch_size]
            results = execute_values(
                cursor,
                insert_query,
                batch,
                template=None,
                page_size=batch_size,
                fetch=True
            )

            # Count inserts vs updates
            for result in results:
                if result[0]:  # xmax = 0 means INSERT
                    records_inserted += 1
                else:  # xmax != 0 means UPDATE
                    records_updated += 1

            logging.info(
                f"Batch {i//batch_size + 1}: "
                f"{records_inserted} inserted, {records_updated} updated so far"
            )

        conn.commit()
        logging.info(
            f"Upsert completed: {records_inserted} inserted, "
            f"{records_updated} updated"
        )

    except Exception as e:
        conn.rollback()
        logging.error(f"Upsert failed: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    return records_inserted, records_updated


def refresh_materialized_views():
    """
    Refresh all materialized views in the correct dependency order.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    materialized_views = [
        'mv_alerts',
        'mv_alert_details',
        'mv_entities'
    ]

    try:
        for mv in materialized_views:
            logging.info(f"Refreshing {mv}...")
            # Use CONCURRENTLY to avoid locking (if indexes exist)
            try:
                cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
            except psycopg2.Error as e:
                # Fallback to non-concurrent if concurrent fails
                logging.warning(f"Concurrent refresh failed for {mv}, using non-concurrent")
                cursor.execute(f"REFRESH MATERIALIZED VIEW {mv}")

            conn.commit()
            logging.info(f"{mv} refreshed successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"MV refresh failed: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def update_sync_metadata(
    sync_id: str,
    file_name: str,
    file_path: str = None,
    file_size_bytes: int = None,
    status: str = 'IN_PROGRESS',
    records_total: int = None,
    records_inserted: int = None,
    records_updated: int = None,
    records_failed: int = None,
    error_message: str = None,
    validation_errors: list = None
):
    """
    Update or insert sync metadata.
    """
    conn = get_postgres_connection()
    cursor = conn.cursor()

    try:
        # Calculate duration if completing
        if status in ['SUCCESS', 'FAILED', 'PARTIAL_SUCCESS']:
            completed_at = datetime.utcnow()
        else:
            completed_at = None

        # Convert validation errors to JSONB
        validation_errors_json = json.dumps(validation_errors) if validation_errors else None

        cursor.execute("""
            INSERT INTO sync_metadata (
                sync_id, file_name, file_path, file_size_bytes, status,
                started_at, completed_at, records_total, records_inserted,
                records_updated, records_failed, error_message, validation_errors
            ) VALUES (
                %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            ON CONFLICT (sync_id) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                records_total = COALESCE(EXCLUDED.records_total, sync_metadata.records_total),
                records_inserted = COALESCE(EXCLUDED.records_inserted, sync_metadata.records_inserted),
                records_updated = COALESCE(EXCLUDED.records_updated, sync_metadata.records_updated),
                records_failed = COALESCE(EXCLUDED.records_failed, sync_metadata.records_failed),
                error_message = COALESCE(EXCLUDED.error_message, sync_metadata.error_message),
                validation_errors = COALESCE(EXCLUDED.validation_errors, sync_metadata.validation_errors)
        """, (
            sync_id, file_name, file_path, file_size_bytes, status,
            completed_at, records_total, records_inserted, records_updated,
            records_failed, error_message, validation_errors_json
        ))

        conn.commit()
        logging.info(f"Sync metadata updated: {sync_id} - {status}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to update sync metadata: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
