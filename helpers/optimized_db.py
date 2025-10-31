"""
Memory-efficient PostgreSQL operations with optimized batch processing.
"""

import logging
import os
import re
import pandas as pd
import numpy as np
from typing import Tuple, List

try:
    import psycopg2
    from psycopg2.extras import execute_values
    from psycopg2 import sql
    PSYCOPG_AVAILABLE = True
except Exception:
    psycopg2 = None
    execute_values = None
    sql = None
    PSYCOPG_AVAILABLE = False


def _strip_caret_tilde(s: str) -> str:
    """Strip leading '^' and trailing '~' characters."""
    if s is None:
        return s
    try:
        s2 = str(s).strip()
        while s2.startswith('^'):
            s2 = s2[1:]
        while s2.endswith('~'):
            s2 = s2[:-1]
        return s2.strip()
    except Exception:
        return s


def _normalize_cell_value(v):
    """Normalize a cell value for PostgreSQL insertion."""
    # None short-circuit
    if v is None:
        return None

    # Check for pandas NA
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # pandas Timestamp -> Python datetime
    if isinstance(v, pd.Timestamp):
        try:
            return v.to_pydatetime()
        except Exception:
            return None

    # numpy datetime64 -> Python datetime
    if isinstance(v, np.datetime64):
        try:
            return pd.to_datetime(v).to_pydatetime()
        except Exception:
            return None

    # Bytes -> string
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode('utf-8', errors='ignore')
        except Exception:
            v = str(v)

    # String normalization
    if isinstance(v, str):
        s = _strip_caret_tilde(v).strip()
        if s == '' or s.lower() in {'~', 'null', 'none', 'nat', 'nan', '<na>'}:
            return None
        if 'nat' in s.lower():
            return None
        return s

    return v


def prepare_dataframe_for_upsert(
    df: pd.DataFrame,
    table_columns: List[str],
    datetime_columns: List[str] = None
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Prepare DataFrame for database upsert with minimal memory overhead.

    Args:
        df: Input DataFrame
        table_columns: List of valid table column names
        datetime_columns: List of column names that should be datetime type

    Returns:
        (prepared_df, column_list) tuple
    """
    if datetime_columns is None:
        datetime_columns = []

    # Create a view instead of copy where possible
    df_clean = df.copy()  # Unfortunately we need a copy for safety

    # Normalize all cell values in-place
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].map(_normalize_cell_value)

    # Special handling for datetime columns
    for col in datetime_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
            df_clean[col] = df_clean[col].map(
                lambda x: x.to_pydatetime() if pd.notna(x) else None
            )

    # Get intersection of df columns and table columns
    valid_columns = [c for c in table_columns if c in df_clean.columns]

    # Select only valid columns
    df_clean = df_clean[valid_columns]

    return df_clean, valid_columns


def upsert_batch_efficient(
    conn,
    cursor,
    table_schema: str,
    table_name: str,
    df: pd.DataFrame,
    columns: List[str],
    pk: str = 'surat_pengantar_barang',
    batch_size: int = 1000
) -> Tuple[int, int]:
    """
    Perform memory-efficient batch upsert.

    Args:
        conn: PostgreSQL connection
        cursor: Database cursor
        table_schema: Schema name
        table_name: Table name
        df: DataFrame to upsert
        columns: List of column names
        pk: Primary key column name
        batch_size: Batch size for execute_values

    Returns:
        (records_inserted, records_updated)
    """
    if len(df) == 0:
        return (0, 0)

    # Build INSERT query
    cols_ident = sql.SQL(', ').join(sql.Identifier(c) for c in columns)
    update_cols = [c for c in columns if c != pk]

    if update_cols:
        updates_sql = sql.SQL(', ').join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_cols
        )
    else:
        updates_sql = sql.SQL('')

    insert_query = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {} RETURNING (xmax = 0) AS inserted"
    ).format(
        sql.Identifier(table_schema),
        sql.Identifier(table_name),
        cols_ident,
        sql.Identifier(pk),
        updates_sql
    )

    query_str = insert_query.as_string(conn)

    # Convert DataFrame to iterator of tuples to avoid creating full list in memory
    records_inserted = 0
    records_updated = 0

    # Process in batches
    total_rows = len(df)
    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i + batch_size]

        # Convert batch to tuples
        batch_values = [
            tuple(_normalize_cell_value(v) for v in row)
            for row in batch_df.to_numpy()
        ]

        # Execute batch
        try:
            results = execute_values(
                cursor,
                query_str,
                batch_values,
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

            # Log progress
            batch_num = (i // batch_size) + 1
            total_batches = (total_rows + batch_size - 1) // batch_size
            logging.info(
                f"Batch {batch_num}/{total_batches}: "
                f"processed {min(i + batch_size, total_rows)}/{total_rows} rows"
            )

        except Exception as e:
            logging.error(f"Error in batch {i}-{i+batch_size}: {e}")
            raise

    return (records_inserted, records_updated)


def get_table_schema_info(cursor, table_schema: str, table_name: str) -> dict:
    """
    Get table schema information from database.

    Returns:
        dict with 'columns' (list), 'datetime_columns' (list)
    """
    cursor.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_schema, table_name)
    )

    columns = []
    datetime_columns = []

    for row in cursor.fetchall():
        col_name, data_type = row
        columns.append(col_name)

        # Identify datetime columns
        if data_type in ('timestamp', 'timestamp with time zone', 'timestamp without time zone', 'date', 'time'):
            datetime_columns.append(col_name)

    return {
        'columns': columns,
        'datetime_columns': datetime_columns
    }

