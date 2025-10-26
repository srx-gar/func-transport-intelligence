"""
PostgreSQL database operations
"""

import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
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
        # Resolve target table columns from information_schema to avoid schema drift issues
        table_schema = os.getenv('DB_SCHEMA', 'public')
        table_name = os.getenv('DB_TABLE', 'transport_documents')
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_schema, table_name)
        )
        table_columns = {row[0] for row in cursor.fetchall()}
        if not table_columns:
            raise RuntimeError(f"Table {table_schema}.{table_name} has no columns or does not exist")

        # Intersect DataFrame columns with actual table columns
        df_columns = [c for c in df.columns.tolist() if c in table_columns]
        if 'surat_pengantar_barang' not in df_columns:
            raise RuntimeError("Primary key column 'surat_pengantar_barang' is missing from payload and/or table")

        # Convert DataFrame to list of tuples, replacing NaT/NaN with None
        df_clean = df[df_columns].where(pd.notnull(df[df_columns]), None)
        values = [tuple(row) for row in df_clean.to_numpy()]

        logging.info(f"Upserting {len(values)} records to {table_schema}.{table_name} (cols={len(df_columns)})")

        # Build column list and update list (exclude primary key only)
        update_cols = [col for col in df_columns if col != 'surat_pengantar_barang']

        # Build INSERT ... ON CONFLICT query
        insert_query = f"""
            INSERT INTO {table_schema}.{table_name} ({', '.join(df_columns)})
            VALUES %s
            ON CONFLICT (surat_pengantar_barang)
            DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])}
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
            except psycopg2.Error:
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


def _ensure_sync_metadata_table(cursor):
    """
    Ensure the sync_metadata table exists in the configured schema. This is a best-effort fallback
    for environments where migrations haven't been applied.
    """
    table_schema = os.getenv('DB_SCHEMA', 'public')

    # Create schema if not exists (safe in many environments)
    try:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(table_schema)))
    except Exception:
        # If creating schema fails (likely because it already exists or insufficient perms), continue
        logging.debug("Could not create schema or schema already exists - continuing")

    create_table_sql = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.sync_metadata (
            id SERIAL PRIMARY KEY,
            sync_id VARCHAR(80) UNIQUE NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            file_path TEXT,
            file_size_bytes BIGINT,
            status VARCHAR(20) NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMP,
            records_total INTEGER,
            records_inserted INTEGER,
            records_updated INTEGER,
            records_failed INTEGER,
            error_message TEXT,
            validation_errors JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """
    ).format(sql.Identifier(table_schema))

    try:
        cursor.execute(create_table_sql)
        # Create a simple index for status to support queries
        try:
            cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS idx_sync_metadata_status ON {}.sync_metadata(status)").format(sql.Identifier(table_schema)))
        except Exception:
            logging.debug("Could not create index idx_sync_metadata_status - continuing")
    except Exception as e:
        logging.warning(f"Failed to ensure sync_metadata table exists: {e}")
        # re-raise so caller can handle if necessary
        raise


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

        table_schema = os.getenv('DB_SCHEMA', 'public')

        insert_stmt = f"""
            INSERT INTO {table_schema}.sync_metadata (
                sync_id, file_name, file_path, file_size_bytes, status,
                started_at, completed_at, records_total, records_inserted,
                records_updated, records_failed, error_message, validation_errors
            ) VALUES (
                %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            ON CONFLICT (sync_id) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                records_total = COALESCE(EXCLUDED.records_total, {table_schema}.sync_metadata.records_total),
                records_inserted = COALESCE(EXCLUDED.records_inserted, {table_schema}.sync_metadata.records_inserted),
                records_updated = COALESCE(EXCLUDED.records_updated, {table_schema}.sync_metadata.records_updated),
                records_failed = COALESCE(EXCLUDED.records_failed, {table_schema}.sync_metadata.records_failed),
                error_message = COALESCE(EXCLUDED.error_message, {table_schema}.sync_metadata.error_message),
                validation_errors = COALESCE(EXCLUDED.validation_errors, {table_schema}.sync_metadata.validation_errors)
        """

        try:
            cursor.execute(insert_stmt, (
                sync_id, file_name, file_path, file_size_bytes, status,
                completed_at, records_total, records_inserted, records_updated,
                records_failed, error_message, validation_errors_json
            ))
        except psycopg2.errors.UndefinedTable:
            # Table doesn't exist - try to create it and retry once
            logging.warning("sync_metadata table not found, attempting to create it and retry")
            try:
                _ensure_sync_metadata_table(cursor)
                conn.commit()
                # retry the insert
                cursor.execute(insert_stmt, (
                    sync_id, file_name, file_path, file_size_bytes, status,
                    completed_at, records_total, records_inserted, records_updated,
                    records_failed, error_message, validation_errors_json
                ))
            except Exception as e:
                conn.rollback()
                logging.error(f"Failed to create/insert into sync_metadata: {e}")
                raise

        conn.commit()
        logging.info(f"Sync metadata updated: {sync_id} - {status}")

    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to update sync metadata: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
