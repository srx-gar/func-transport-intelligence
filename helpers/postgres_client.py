"""
PostgreSQL database operations
"""

# psycopg2 is an optional runtime dependency for local dry-run / testing.
# Import it lazily and fall back to no-op stubs when unavailable so the
# function_app and local runner can be imported without a compiled driver.
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

import pandas as pd
import os
import logging
import re
import numpy as np
import difflib
from typing import Optional


def get_postgres_connection():
    """
    Get PostgreSQL connection using environment variables.
    """
    # Support both connection string and individual parameters
    conn_str = os.getenv('POSTGRES_CONNECTION_STRING')

    if conn_str:
        conn = psycopg2.connect(conn_str)
    else:
        host = os.getenv('DB_HOST', 'localhost')
        port = os.getenv('DB_PORT', '5432')
        dbname = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')

        # Allow explicit override from env (e.g. DB_SSL_MODE=disable/require/verify-full)
        sslmode = os.getenv('DB_SSL_MODE')
        if not sslmode:
            # If pointing to localhost or the Docker-mapped port, disable SSL by default
            host_lower = (host or '').lower()
            if host_lower in ('localhost', '127.0.0.1', '::1') or host_lower.endswith('.local') or host_lower.startswith('127.'):
                sslmode = 'disable'
            else:
                sslmode = 'require'

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=30,  # 30 second connection timeout
            keepalives=1,        # Enable TCP keepalives
            keepalives_idle=60,  # Start keepalives after 60s of inactivity
            keepalives_interval=10,  # Send keepalive every 10s
            keepalives_count=5   # Drop connection after 5 failed keepalives
        )

    # Set statement timeout to prevent individual queries from running too long
    # This helps detect stuck queries faster
    try:
        cursor = conn.cursor()
        # Set statement timeout to 10 minutes (600000ms) to handle large batches
        # Each chunk may have 100K+ rows which takes several minutes to upsert
        cursor.execute("SET statement_timeout = '600000'")
        cursor.close()
    except Exception as e:
        logging.warning(f"Could not set statement timeout: {e}")

    return conn


def _canonicalize_colname(col: str) -> str:
    """Lightweight canonicalization: remove BOM, non-alnum -> underscore, collapse underscores, lowercase."""
    if col is None:
        return ''
    col = str(col)
    col = col.replace('\ufeff', '')
    col = col.strip()
    col = re.sub(r"[^0-9a-zA-Z]+", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_").lower()


def _build_insert_query_string(conn, table_schema, table_name, df_columns, pk='surat_pengantar_barang') -> str:
    """Build and return the INSERT ... ON CONFLICT SQL string (for testing/inspection).

    This function uses psycopg2.sql to properly quote identifiers.
    It requires an active connection for as_string() when converting SQL objects to text.
    """
    if not df_columns:
        raise ValueError("No columns supplied to build insert query")

    cols_ident = sql.SQL(', ').join(sql.Identifier(c) for c in df_columns)
    update_cols = [c for c in df_columns if c != pk]
    if update_cols:
        updates_sql = sql.SQL(', ').join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
        )
    else:
        updates_sql = sql.SQL('')

    insert_query = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {} RETURNING (xmax = 0) AS inserted").format(
        sql.Identifier(table_schema), sql.Identifier(table_name), cols_ident, sql.Identifier(pk), updates_sql
    )

    return insert_query.as_string(conn)


def _quote_identifier(name: str) -> str:
    """Return a safely quoted SQL identifier (double quotes, escape internal quotes)."""
    if name is None:
        raise ValueError('Identifier name is None')
    return '"' + str(name).replace('"', '""') + '"'


def build_insert_query_str_no_conn(table_schema: str, table_name: str, df_columns: list,
                                   pk: str = 'surat_pengantar_barang') -> str:
    """Build INSERT ... ON CONFLICT SQL string using safe quoting without a DB connection.

    This is useful for diagnostics or environments where a DB connection isn't available.
    """
    if not df_columns:
        raise ValueError('No columns supplied')

    cols = ', '.join(_quote_identifier(c) for c in df_columns)
    update_cols = [c for c in df_columns if c != pk]
    if update_cols:
        updates = ', '.join(f"{_quote_identifier(c)} = EXCLUDED.{_quote_identifier(c)}" for c in update_cols)
    else:
        updates = ''

    insert_q = f"INSERT INTO {_quote_identifier(table_schema)}.{_quote_identifier(table_name)} ({cols}) VALUES %s ON CONFLICT ({_quote_identifier(pk)}) DO UPDATE SET {updates} RETURNING (xmax = 0) AS inserted"
    return insert_q


def _strip_caret_tilde(s: str) -> Optional[str]:
    """Strip leading '^' characters and trailing '~' characters from a string and trim whitespace.

    This is intentionally conservative: it only removes caret (^) characters at the
    start and tilde (~) characters at the end. It does not remove these characters
    if they appear in the middle of the string.
    """
    if s is None:
        return s
    try:
        s2 = str(s)
    except Exception:
        return s
    # Strip outer whitespace first, then remove markers, then trim again
    s2 = s2.strip()
    # remove all leading '^'
    while s2.startswith('^'):
        s2 = s2[1:]
    # remove all trailing '~'
    while s2.endswith('~'):
        s2 = s2[:-1]
    return s2.strip()


def normalize_prefixed_tilde(value):
    """Recursively normalize a value by stripping leading '^' and trailing '~' from strings.

    - If value is a dict, returns a new dict with the same keys and normalized values.
    - If value is a list/tuple/set, returns a list of normalized items (tuples/sets are converted to lists).
    - If value is bytes/bytearray, decode then normalize the resulting string.
    - If value is a string, strip leading '^' and trailing '~' and trim; if the result is empty, return None.
    - Otherwise, return the original value.

    This helper is useful when ingesting payloads that include marker tokens like '^...~'.
    """
    # dict
    if isinstance(value, dict):
        return {k: normalize_prefixed_tilde(v) for k, v in value.items()}
    # list/tuple/set -> list
    if isinstance(value, (list, tuple, set)):
        return [normalize_prefixed_tilde(v) for v in value]
    # bytes -> decode
    if isinstance(value, (bytes, bytearray)):
        try:
            s = value.decode('utf-8', errors='ignore')
        except Exception:
            s = str(value)
        s = _strip_caret_tilde(s)
        return s if s != '' else None
    # string
    if isinstance(value, str):
        s = _strip_caret_tilde(value)
        if s == '':
            return None
        return s
    # fallback
    return value


def upsert_to_postgres(sync_id: str, df: pd.DataFrame) -> tuple:
    """
    Upsert DataFrame to PostgreSQL using INSERT ... ON CONFLICT.

    Returns (records_inserted, records_updated)
    """
    if len(df) == 0:
        logging.warning("Empty DataFrame, skipping upsert")
        return (0, 0)

    # Defensive pre-clean: sanitize incoming DataFrame column tokens which may contain
    # stray delimiter artifacts (e.g. '^tanggal_buat~' or 'surat_pengantar_brg~') coming
    # from source headers in case parser normalization wasn't applied in the runtime.
    try:
        cleaned_cols = []
        for c in df.columns.tolist():
            s = str(c)
            s = s.replace('\\', '')
            s = s.strip()
            if s.startswith('^'):
                s = s.lstrip('^')
            if s.endswith('~'):
                s = s.rstrip('~')
            cleaned_cols.append(s)
        df = df.copy()
        df.columns = cleaned_cols
        logging.debug('Pre-cleaned DataFrame columns for upsert: %s', cleaned_cols[:20])
    except Exception:
        logging.exception('Failed to pre-clean DataFrame columns; proceeding with original column names')

    if not PSYCOPG_AVAILABLE:
        # Provide a helpful error when attempting a DB upsert without psycopg2
        raise RuntimeError("psycopg2 is not available in the environment; install psycopg2-binary or use --to-db only when the DB driver is installed")

    # Determine target table early so fallback paths can build diagnostic SQL
    table_schema = os.getenv('DB_SCHEMA', 'public')
    table_name = os.getenv('DB_TABLE', 'transport_documents')
    fallback_columns = list(df.columns)

    # Attempt to get a DB connection. If this fails, print a fully-formed
    # INSERT ... ON CONFLICT query using the incoming DataFrame columns so
    # developers can inspect the SQL even when the DB is unreachable.
    try:
        conn = get_postgres_connection()
        # Print connection/environment info for debugging so we can verify
        # the function is talking to the expected DB instance.
        try:
            dbg_cursor = conn.cursor()
            try:
                dbg_cursor.execute("SELECT current_database(), current_schema(), inet_server_addr(), inet_server_port()")
                db_info = dbg_cursor.fetchone()
                # Keep server info at DEBUG level to avoid noisy production logs
                # debug suppressed: DB connection info
            except Exception:
                # inet_server_addr may not be available in all environments; fallback to env vars
                logging.debug('inet_server_addr not available in this environment')
                # debug suppressed: DB connection established (env)
            finally:
                dbg_cursor.close()
        except Exception:
            # silently ignore debug-only fetch failures
            pass
    except Exception as conn_exc:
        # Connection failed; keep a concise warning and skip verbose debug output
        logging.warning('Could not connect to DB for upsert (will raise): %s', conn_exc)
        try:
            insert_query_str = build_insert_query_str_no_conn(table_schema, table_name, fallback_columns, pk='surat_pengantar_barang')
            # debug suppressed: built fallback INSERT query (no-conn)
        except Exception as e2:
            logging.debug('Failed to build fallback insert query: %s', e2)
        # Re-raise original connection exception so upstream behavior remains unchanged
        raise

    cursor = conn.cursor()

    records_inserted = 0
    records_updated = 0

    import sys
    import time

    try:
        # Resolve target table columns from information_schema to avoid schema drift issues
        table_schema = os.getenv('DB_SCHEMA', 'public')
        table_name = os.getenv('DB_TABLE', 'transport_documents')
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_schema, table_name)
        )
        table_columns = [row[0] for row in cursor.fetchall()]
        table_columns_set = set(table_columns)
        if not table_columns:
            raise RuntimeError(f"Table {table_schema}.{table_name} has no columns or does not exist")

        # Build canonical mapping of table column names -> their canonical forms
        canonical_table_map = {_canonicalize_colname(tc): tc for tc in table_columns}

        # Map incoming df columns to actual table column names using canonicalization
        mapped_columns = []
        used_table_cols = set()
        mapping_issues = []
        # Common token-level abbreviation expansions (left intentionally small; extend as needed)
        ABBREV_MAP = {
            'brg': 'barang',
            'tgl': 'tanggal',
            'wkt': 'waktu',
            'no': 'nomor',
            'qty': 'quantity',
            'std': 'standard',
            'kbm': 'kilometer',
        }
        for col in df.columns.tolist():
            # Defensive cleanup: remove common separator remnants that sometimes remain in header tokens
            cleaned_col = str(col).replace('~', '').replace('|', '').replace('^', '').strip()
            # Quick exact match (after simple cleanup)
            if cleaned_col in table_columns_set:
                mapped = cleaned_col
            else:
                # Tokenize and expand known abbreviations to increase match probability
                canon = _canonicalize_colname(cleaned_col)
                tokens = canon.split('_') if canon else []
                expanded_tokens = [ABBREV_MAP.get(t, t) for t in tokens]
                expanded = '_'.join(expanded_tokens)
                # Try direct canonical mapping first, then expanded form
                mapped = canonical_table_map.get(canon) or canonical_table_map.get(expanded)
                # If still not found, try fuzzy matching against canonical table keys
                if not mapped:
                    candidate_keys = list(canonical_table_map.keys())
                    # get_close_matches works on strings; use a moderate cutoff
                    close = difflib.get_close_matches(canon, candidate_keys, n=1, cutoff=0.8)
                    if close:
                        mapped = canonical_table_map.get(close[0])
                        logging.warning("Fuzzy-mapped incoming column '%s' -> '%s'", col, mapped)
            if mapped and mapped not in used_table_cols:
                # skip invalid identifiers defensively
                if not re.match(r'^[A-Za-z0-9_]+$', mapped):
                    mapping_issues.append((col, mapped))
                else:
                    mapped_columns.append(mapped)
                    used_table_cols.add(mapped)
            else:
                logging.debug("Dropping/ignoring incoming column '%s' (no match to table) or duplicate mapping", col)

        # Build a rename map from original incoming column names -> mapped table column names
        # so we can safely select and preserve ordering. This ensures DataFrame columns match
        # the DB column identifiers used in the INSERT statement.
        rename_map = {}
        used = set()
        for orig_col in df.columns.tolist():
            cleaned_col = str(orig_col).replace('~', '').replace('|', '').replace('^', '').strip()
            # Try to find the mapped table column for this original column
            # Reuse same matching strategy as above but prefer exact mapped_columns membership
            mapped = None
            if cleaned_col in table_columns_set and cleaned_col not in used:
                mapped = cleaned_col
            else:
                canon = _canonicalize_colname(cleaned_col)
                mapped = canonical_table_map.get(canon)
                if not mapped:
                    # expand abbreviations
                    tokens = canon.split('_') if canon else []
                    expanded_tokens = [ABBREV_MAP.get(t, t) for t in tokens]
                    expanded = '_'.join(expanded_tokens)
                    mapped = canonical_table_map.get(expanded)
            if mapped and mapped not in used:
                rename_map[orig_col] = mapped
                used.add(mapped)

        if rename_map:
            logging.info('Renaming incoming DataFrame columns for upsert: %s', list(rename_map.items())[:20])
            df = df.rename(columns=rename_map)

        # Preserve the table's column ordering for INSERT
        df_columns = [c for c in table_columns if c in mapped_columns]

        # Defensive validation: ensure df_columns are valid SQL identifiers.
        # Some incoming header tokens can contain stray characters (e.g. '~') that
        # slipped through earlier normalization; make a final pass to drop or
        # remap non-matching items so we don't generate malformed SQL.
        valid_cols = []
        for c in df_columns:
            # Allow only simple identifier characters (letters, digits, underscore)
            if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', c):
                valid_cols.append(c)
                continue
            # Try canonical mapping as a best-effort remediation
            canon = _canonicalize_colname(c)
            remap = canonical_table_map.get(canon)
            if remap and re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', remap):
                logging.warning("Remapping column '%s' -> '%s' to produce safe identifier", c, remap)
                valid_cols.append(remap)
                continue
            logging.warning("Dropping column '%s' from INSERT because it is not a valid SQL identifier", c)

        # Use the validated column list
        df_columns = valid_cols

        # Log any mapping issues
        if mapping_issues:
            logging.warning('Some incoming columns mapped to invalid table identifiers and were skipped: %s',
                            mapping_issues)

        if 'surat_pengantar_barang' not in df_columns:
            raise RuntimeError("Primary key column 'surat_pengantar_barang' is missing from payload and/or table")

        # Convert DataFrame to list of tuples, replacing NaT/NaN with None
        df_clean = df[df_columns].where(pd.notnull(df[df_columns]), None).copy()

        # Normalize cell values to Python-native types compatible with psycopg2.
        # Use a robust function that handles:
        # - pandas/NumPy NA types -> None
        # - pandas.Timestamp / numpy.datetime64 -> Python datetime
        # - strings containing 'nat' or placeholder tokens ('~','NULL','None') -> None
        def _normalize_cell(v):
            # None short-circuit
            if v is None:
                return None

            # pd.isna handles NaT/NaN/None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass

            # pandas Timestamp -> py datetime
            if isinstance(v, pd.Timestamp):
                try:
                    return v.to_pydatetime()
                except Exception:
                    return None

            # numpy datetime64 -> py datetime
            if isinstance(v, np.datetime64):
                try:
                    return pd.to_datetime(v).to_pydatetime()
                except Exception:
                    return None

            # Strings: treat empty/placeholder/NAT-like values as None
            if isinstance(v, (bytes, bytearray)):
                try:
                    v = v.decode('utf-8', errors='ignore')
                except Exception:
                    v = str(v)

            if isinstance(v, str):
                # Normalize and strip marker tokens like leading '^' and trailing '~'
                s = _strip_caret_tilde(v)
                s = s.strip()
                if s == '':
                    return None
                low = s.lower()
                if low in {'~', 'null', 'none', 'nat', '<na>'}:
                    return None
                # if 'nat' appears anywhere, treat as missing
                if 'nat' in low:
                    return None
                return s

            return v

        # Use apply with map to avoid FutureWarning for applymap
        df_clean = df_clean.apply(lambda col: col.map(_normalize_cell))

        # Final sanitization pass: detect any lingering 'NaT' substrings or other NaT-like values
        # and coerce them to None; also ensure datetime-like values are Python datetimes.
        nat_issues = []
        def _final_fix(v):
            # None/null remains None
            if v is None:
                return None
            # Strings containing nat anywhere or <NA> -> None
            if isinstance(v, str):
                v_lower = v.lower()
                if 'nat' in v_lower or v_lower == '<na>':
                    return None
            # pandas/np NA
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            # pandas Timestamp -> python datetime
            if isinstance(v, pd.Timestamp):
                try:
                    return v.to_pydatetime()
                except Exception:
                    return None
            # numpy datetime64
            if isinstance(v, np.datetime64):
                try:
                    return pd.to_datetime(v).to_pydatetime()
                except Exception:
                    return None
            return v

        # Apply final fix and collect a few issue samples for logging
        samples = []
        for col in df_clean.columns:
            # apply per-column mapping to keep memory usage moderate
            series = df_clean[col].map(_final_fix)
            # find samples where replacement occurred
            mask = df_clean[col].notnull() & series.isnull()
            if mask.any():
                idxs = list(series[mask].index[:5])
                for ii in idxs:
                    samples.append((ii, col, df_clean.at[ii, col]))
            df_clean[col] = series

        if samples:
            logging.warning('Sanitized %d NaT-like cells before upsert; samples: %s', len(samples), samples[:8])

        # Targeted sanitization for datetime-like target columns: coerce string 'NaT' and
        # unparseable date/time strings to None, and convert parseable strings to Python datetimes.
        datetime_candidates = [
            c for c in df_columns if any(k in c for k in ('waktu', 'tanggal', 'date', '_at', 'posting'))
        ]
        datetime_issues = []
        for col in datetime_candidates:
            if col not in df_clean.columns:
                continue
            # If column values are strings, try to parse; if unparseable or contain 'nat', set None
            def _fix_dt(v):
                if v is None:
                    return None
                # if already python datetime, keep
                try:
                    import datetime as _dt
                    if isinstance(v, _dt.datetime):
                        return v
                except Exception:
                    pass
                # pandas Timestamp
                if isinstance(v, pd.Timestamp):
                    try:
                        return v.to_pydatetime()
                    except Exception:
                        return None
                # numpy datetime64
                if isinstance(v, np.datetime64):
                    try:
                        return pd.to_datetime(v).to_pydatetime()
                    except Exception:
                        return None
                # strings containing nat anywhere or '<NA>'
                if isinstance(v, str):
                    v_lower = v.lower()
                    if 'nat' in v_lower or v_lower == '<na>':
                        return None
                # attempt parsing strings
                if isinstance(v, str):
                    parsed = pd.to_datetime(v, errors='coerce')
                    if pd.isna(parsed):
                        # record issue and return None
                        datetime_issues.append((col, v))
                        return None
                    return parsed.to_pydatetime()
                # fallback - return as-is
                return v

            df_clean[col] = df_clean[col].map(_fix_dt)

        if datetime_issues:
            logging.warning('Datetime parsing issues before upsert (sample 8): %s', datetime_issues[:8])

        # Final defensive coercion: make absolutely sure datetime candidate columns
        # contain only Python datetimes or None. This prevents string values like
        # 'NaT' from being sent to Postgres where they would be cast as 'NaT'::timestamp
        # and cause an error.
        def _finalize_datetime_cell(v):
            if v is None:
                return None
            # treat strings containing 'nat' (case-insensitive) or '<NA>' as missing
            if isinstance(v, str):
                v_lower = v.lower()
                if 'nat' in v_lower or v_lower == '<na>':
                    return None
            # already a python datetime
            try:
                import datetime as _dt
                if isinstance(v, _dt.datetime):
                    return v
            except Exception:
                pass
            # pandas Timestamp
            if isinstance(v, pd.Timestamp):
                try:
                    return v.to_pydatetime()
                except Exception:
                    return None
            # numpy datetime64
            if isinstance(v, np.datetime64):
                try:
                    return pd.to_datetime(v).to_pydatetime()
                except Exception:
                    return None
            # try parsing strings or other types
            try:
                parsed = pd.to_datetime(v, errors='coerce')
                if pd.isna(parsed):
                    return None
                return parsed.to_pydatetime()
            except Exception:
                return None

        for col in datetime_candidates:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].map(_finalize_datetime_cell)

        # Normalize primary key values to remove stray tokens like leading '^' and trailing '~'
        if 'surat_pengantar_barang' in df_clean.columns:
            def _normalize_pk(v):
                if v is None:
                    return None
                if isinstance(v, str):
                    # use shared helper to remove leading '^' and trailing '~'
                    s = _strip_caret_tilde(v)
                    s = s.strip()
                    if s == '':
                        return None
                    return s
                # bytes
                if isinstance(v, (bytes, bytearray)):
                    try:
                        s = v.decode('utf-8', errors='ignore')
                    except Exception:
                        s = str(v)
                    s = _strip_caret_tilde(s)
                    s = s.strip()
                    if s == '':
                        return None
                    return s
                return v

            df_clean['surat_pengantar_barang'] = df_clean['surat_pengantar_barang'].map(_normalize_pk)

        # Build the values tuples from the sanitized DataFrame; ensure any lingering
        # string tokens that look like 'nat' or '<NA>' are coerced to None as a last resort.
        def _final_sanitize_for_sql(x):
            # pandas/NumPy NA-like
            try:
                if pd.isna(x):
                    return None
            except Exception:
                pass
            # pandas Timestamp -> python datetime
            if isinstance(x, pd.Timestamp):
                try:
                    return x.to_pydatetime()
                except Exception:
                    return None
            # numpy datetime64 -> python datetime
            if isinstance(x, np.datetime64):
                try:
                    return pd.to_datetime(x).to_pydatetime()
                except Exception:
                    return None
            # strings containing 'nat' or '<NA>' -> None
            if isinstance(x, str):
                # Strip marker tokens first
                sx = _strip_caret_tilde(x)
                sx_lower = sx.lower()
                if 'nat' in sx_lower or sx_lower == '<na>':
                    return None
                if sx == '':
                    return None
                return sx
            return x

        values = []
        for row in df_clean.to_numpy():
            sanitized_row = tuple(_final_sanitize_for_sql(v) for v in row)
            values.append(sanitized_row)

        logging.info(f"Upserting {len(values)} records to {table_schema}.{table_name} (cols={len(df_columns)})")

        # DEBUG: Check if any of the primary keys already exist in the database
        if values:
            try:
                # Get first few primary keys from the data
                pk_col_index = df_columns.index('surat_pengantar_barang')
                sample_pks = [row[pk_col_index] for row in values[:10]]
                all_pks = [row[pk_col_index] for row in values]

                # CRITICAL: Check for NULL primary keys before attempting upsert
                null_pks = [
                    (i, pk) for i, pk in enumerate(all_pks)
                    if pk is None
                    or pk == ''
                    or (isinstance(pk, str) and pk.strip() in ('', 'None', 'null', 'NULL', 'nan', 'NaN', 'NAN', '<NA>'))
                    or str(pk).strip() in ('', 'None', 'null', 'NULL', 'nan', 'NaN', 'NAN', '<NA>')
                ]
                if null_pks:
                    logging.error(
                        "⚠️  WARNING: Found %d NULL/empty primary keys in batch - FILTERING THEM OUT",
                        len(null_pks)
                    )
                    # Log first few for debugging
                    for i, pk in null_pks[:5]:
                        logging.error(f"  NULL PK at batch index {i}: value='{pk}' (type={type(pk).__name__})")
                        # Log the full row for this bad PK
                        logging.error(f"  Full row: {values[i][:10]}...")  # First 10 columns

                    # FILTER OUT rows with NULL primary keys instead of raising error
                    null_pk_indices = {i for i, pk in null_pks}
                    values_filtered = [row for i, row in enumerate(values) if i not in null_pk_indices]

                    logging.error(
                        f"  Filtered out {len(null_pks)} rows with NULL primary keys. "
                        f"Continuing with {len(values_filtered)} valid rows."
                    )

                    # Update values to use the filtered list
                    values = values_filtered

                    # If all rows were filtered out, return early with zero inserts/updates
                    if len(values) == 0:
                        logging.warning("All rows had NULL primary keys - nothing to upsert")
                        cursor.close()
                        conn.close()
                        return (0, 0)

                # Build canonical forms (alphanumeric only, lowercase) to compare robustly
                def _canon(pk):
                    try:
                        s = str(pk)
                        # use helper to remove marker tokens before canonicalization
                        s = _strip_caret_tilde(s)
                        s = re.sub(r'[^0-9a-zA-Z]+', '', s)
                        return s.lower()
                    except Exception:
                        return str(pk)

                sample_pks_canon = [_canon(p) for p in sample_pks]
                all_pks_canon = [_canon(p) for p in all_pks]

                # Query DB for matches using canonicalized comparison on the server side
                # Use lower(regexp_replace(...,'[^0-9a-zA-Z]','','g')) to strip non-alnum and lowercase
                cursor.execute(
                    f"SELECT surat_pengantar_barang, lower(regexp_replace(surat_pengantar_barang, '[^0-9a-zA-Z]+', '', 'g')) as canon FROM {_quote_identifier(table_schema)}.{_quote_identifier(table_name)} "
                    f"WHERE lower(regexp_replace(surat_pengantar_barang, '[^0-9a-zA-Z]+', '', 'g')) = ANY(%s)",
                    (sample_pks_canon,)
                )
                rows = cursor.fetchall()
                existing_pks = [r[0] for r in rows]
                existing_pks_canon = [r[1] for r in rows]

                # Also get total count by canonical match for the whole batch
                cursor.execute(
                    f"SELECT COUNT(*) FROM {_quote_identifier(table_schema)}.{_quote_identifier(table_name)} "
                    f"WHERE lower(regexp_replace(surat_pengantar_barang, '[^0-9a-zA-Z]+', '', 'g')) = ANY(%s)",
                    (all_pks_canon,)
                )
                existing_count = cursor.fetchone()[0]

                # Pre-check found existing_count; do not spam logs with details unless DEBUG enabled
                # debug suppressed: detailed pre-check stats
            except Exception as e:
                logging.warning(f"DEBUG check failed: {e}")

        # Use the DB-backed builder to get a safe query string (ensures proper quoting)
        try:
            insert_query_str = _build_insert_query_string(conn, table_schema, table_name, df_columns, pk='surat_pengantar_barang')
            # debug suppressed: built DB-backed INSERT query
        except Exception:
            # Fallback to no-conn builder if for any reason the conn-backed builder fails
            logging.exception('Failed to build insert query using DB-backed SQL builder; falling back to string builder')
            insert_query_str = build_insert_query_str_no_conn(table_schema, table_name, df_columns, pk='surat_pengantar_barang')
            # debug suppressed: built no-conn INSERT query

        # Execute batch insert with pagination
        # Reduce batch size for tables with many columns to avoid timeout
        # 86 columns * 500 rows = 43,000 parameters (well under PostgreSQL's 65,535 limit)
        batch_size = 500

        total_batches = (len(values) + batch_size - 1) // batch_size
        logging.info(f"Starting batch upsert: {len(values)} rows in {total_batches} batches of {batch_size}")

        batch_num = 0  # Initialize before loop
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            batch_num = i // batch_size + 1

            # Check connection health every 10 batches
            if batch_num % 10 == 0:
                try:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                except Exception as conn_check_error:
                    logging.error(f"❌ Connection health check failed at batch {batch_num}: {conn_check_error}")
                    sys.stdout.flush()
                    raise

            # Log the query for debugging (only first batch)
            if i == 0:
                # Only log the first-batch size at INFO; avoid logging parameter tuples
                logging.info('Executing upsert first batch (size=%s)', len(batch))

            batch_start = time.time()
            try:
                results = execute_values(
                    cursor,
                    insert_query_str,
                    batch,
                    template=None,
                    page_size=batch_size,
                    fetch=True
                )
            except Exception as batch_error:
                logging.error(f"❌ Batch {batch_num}/{total_batches} failed: {batch_error}", exc_info=True)
                sys.stdout.flush()
                sys.stderr.flush()
                raise

            batch_duration = time.time() - batch_start

            # Count inserts vs updates
            batch_inserts = 0
            batch_updates = 0
            for result in results:
                if result[0]:  # xmax = 0 means INSERT
                    records_inserted += 1
                    batch_inserts += 1
                else:  # xmax != 0 means UPDATE
                    records_updated += 1
                    batch_updates += 1

            logging.info('Batch %s/%s: %s inserted, %s updated (%.2fs) | Total: %s inserted, %s updated',
                        batch_num, total_batches, batch_inserts, batch_updates, batch_duration,
                        records_inserted, records_updated)

            # Force log flush every 10 batches to ensure progress is visible
            if batch_num % 10 == 0:
                sys.stdout.flush()
                sys.stderr.flush()

        # Log immediately after loop exits to confirm completion
        logging.info(f"🔄 Batch loop completed. Processed {batch_num}/{total_batches} batches successfully.")
        sys.stdout.flush()
        sys.stderr.flush()

        # All batches executed, now commit
        logging.info(f"✅ All {total_batches} batches executed. Committing transaction...")
        sys.stdout.flush()
        sys.stderr.flush()

        commit_start = time.time()
        conn.commit()
        commit_duration = time.time() - commit_start

        logging.info(f"✅ Upsert committed in {commit_duration:.2f}s: {records_inserted} inserted, {records_updated} updated")
        sys.stdout.flush()
        sys.stderr.flush()

    except Exception as e:
        logging.error(f"❌ Upsert failed: {str(e)}", exc_info=True)
        sys.stdout.flush()
        sys.stderr.flush()
        conn.rollback()
        logging.error(f"Transaction rolled back")
        sys.stdout.flush()
        raise
    finally:
        cursor.close()
        conn.close()

    return records_inserted, records_updated


def update_sync_metadata(sync_id: str, file_name: str = None, file_path: str = None,
                         file_size_bytes: int = None, status: str = None,
                         records_total: int = None, records_inserted: int = None,
                         records_updated: int = None, records_failed: int = None,
                         error_message: str = None, validation_errors: list = None):
    """Update or insert a sync metadata record.

    In environments where psycopg2 is not available (local dry-run), this will
    log the metadata instead of performing DB operations. When psycopg2 is
    available, it will perform a simple upsert into a table `sync_metadata`
    if that table exists (best-effort).
    """
    # Compute validation errors count and prepare a JSON payload for the DB when present
    validation_errors_count = len(validation_errors) if validation_errors else 0
    payload = {
        'sync_id': sync_id,
        'file_name': file_name,
        'file_path': file_path,
        'file_size_bytes': file_size_bytes,
        'status': status,
        'records_total': records_total,
        'records_inserted': records_inserted,
        'records_updated': records_updated,
        'records_failed': records_failed,
        'error_message': error_message,
        'validation_errors_count': validation_errors_count,
    }
    logging.info('update_sync_metadata: %s', payload)
    # Ensure db_info exists in all code paths for later diagnostic printing
    db_info = None

    # Build a DB-friendly representation of the full validation errors (if any).
    # Prefer psycopg2.extras.Json adapter when available so the DB receives proper JSONB value;
    # otherwise fall back to a JSON string.
    validation_errors_param = None
    if validation_errors_count > 0:
        try:
            from psycopg2.extras import Json as _Json
            validation_errors_param = _Json(validation_errors)
        except Exception:
            try:
                import json as _json
                validation_errors_param = _json.dumps(validation_errors)
            except Exception:
                validation_errors_param = None

    if not PSYCOPG_AVAILABLE:
        # Try a runtime import fallback in case psycopg2 was not available at module import
        try:
            import importlib
            _psycopg2 = importlib.import_module('psycopg2')
            globals()['psycopg2'] = _psycopg2
            # Attempt to also bring in extras if present (not strictly required for this function)
            try:
                globals()['execute_values'] = importlib.import_module('psycopg2.extras').execute_values
            except Exception:
                pass
            try:
                globals()['sql'] = importlib.import_module('psycopg2.sql')
            except Exception:
                pass
            globals()['PSYCOPG_AVAILABLE'] = True
            logging.debug('psycopg2 imported at runtime in update_sync_metadata')
        except Exception:
            logging.debug('psycopg2 not available at runtime; sync_metadata will not be persisted')
            return

    # Best-effort DB persistence: try to insert into a table named sync_metadata
    try:
        # Establish a DB connection and emit some debug info so we can verify the
        # target DB instance (mirrors the diagnostic behaviour in upsert_to_postgres)
        conn = get_postgres_connection()
        try:
            dbg_cursor = conn.cursor()
            # ensure db_info is always defined for later diagnostics
            db_info = None
            try:
                dbg_cursor.execute("SELECT current_database(), current_schema(), inet_server_addr(), inet_server_port()")
                db_info = dbg_cursor.fetchone()
                logging.debug('Connected to DB: %s', db_info)
            except Exception:
                logging.debug('inet_server_addr not available in this environment')
            finally:
                dbg_cursor.close()
        except Exception:
            # If debug-only fetch fails, silently continue; we still have a live conn
            pass

        cur = conn.cursor()

        # Respect configured DB schema to avoid creating the table in the wrong schema
        table_schema = os.getenv('DB_SCHEMA', 'public') or 'public'
        schema_table = f"{_quote_identifier(table_schema)}.{_quote_identifier('sync_metadata')}"

        # Create table if not exists in the configured schema
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema_table} ("
            "sync_id TEXT PRIMARY KEY, "
            "file_name TEXT, "
            "file_path TEXT, "
            "file_size_bytes BIGINT, "
            "status TEXT, "
            "records_total INTEGER, "
            "records_inserted INTEGER, "
            "records_updated INTEGER, "
            "records_failed INTEGER, "
            "error_message TEXT, "
            "validation_errors_count INTEGER, "
            "updated_at TIMESTAMP DEFAULT now()"
            ")"
        )

        # Ensure any historical tables missing newer columns get altered to include them.
        try:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (table_schema, 'sync_metadata')
            )
            existing_cols = {r[0] for r in cur.fetchall()}
        except Exception:
            existing_cols = set()

        expected_columns = {
            'sync_id': 'TEXT',
            'file_name': 'TEXT',
            'file_path': 'TEXT',
            'file_size_bytes': 'BIGINT',
            'status': 'TEXT',
            'records_total': 'INTEGER',
            'records_inserted': 'INTEGER',
            'records_updated': 'INTEGER',
            'records_failed': 'INTEGER',
            'error_message': 'TEXT',
            'validation_errors_count': 'INTEGER',
            # new: full JSONB column to store the validation error details
            'validation_errors': 'JSONB',
            'updated_at': 'TIMESTAMP DEFAULT now()',
        }

        for col, col_type in expected_columns.items():
            if col not in existing_cols:
                try:
                    # Use ALTER TABLE ... ADD COLUMN IF NOT EXISTS where supported
                    cur.execute(f"ALTER TABLE {schema_table} ADD COLUMN IF NOT EXISTS {_quote_identifier(col)} {col_type}")
                    logging.info('Added missing column to sync_metadata: %s', col)
                except Exception as e:
                    logging.warning('Failed to add column %s to %s: %s', col, schema_table, e)

        # Now perform the upsert into the sync_metadata table (schema-qualified).
        # Include the full validation_errors payload when present (as JSONB). If the
        # Json adapter was created above, pass it directly; otherwise pass a JSON string
        # or NULL.
        cur.execute(
            f"INSERT INTO {schema_table} (sync_id, file_name, file_path, file_size_bytes, status, "
            "records_total, records_inserted, records_updated, records_failed, "
            "error_message, validation_errors_count, validation_errors, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (sync_id) DO UPDATE SET "
            "file_name = EXCLUDED.file_name, "
            "file_path = EXCLUDED.file_path, "
            "file_size_bytes = EXCLUDED.file_size_bytes, "
            "status = EXCLUDED.status, "
            "records_total = EXCLUDED.records_total, "
            "records_inserted = EXCLUDED.records_inserted, "
            "records_updated = EXCLUDED.records_updated, "
            "records_failed = EXCLUDED.records_failed, "
            "error_message = EXCLUDED.error_message, "
            "validation_errors_count = EXCLUDED.validation_errors_count, "
            "validation_errors = EXCLUDED.validation_errors, "
            "updated_at = now()",
            (
                sync_id, file_name, file_path, file_size_bytes, status,
                records_total, records_inserted, records_updated, records_failed,
                error_message, validation_errors_count, validation_errors_param
            )
        )

        # Log result of the insert/upsert for diagnostics
        try:
            logging.info('sync_metadata upsert affected rows: %s', cur.rowcount)
        except Exception:
            logging.debug('Could not read cursor.rowcount after sync_metadata upsert')

        # Commit and close, but also emit a single-line machine-readable summary
        try:
            conn.commit()
            # Print to stdout so Azure Functions host includes it reliably in logs
            try:
                import json as _json
                payload_short = {
                    'sync_id': sync_id,
                    'status': status,
                    'file_name': file_name,
                    'records_total': records_total,
                    'records_inserted': records_inserted,
                    'records_updated': records_updated,
                    'records_failed': records_failed,
                }
                # Include a brief DB info tag when available
                db_tag = None
                try:
                    if 'db_info' in locals() and db_info:
                        db_tag = db_info
                except Exception:
                    db_tag = None
                print('SYNC_METADATA ' + _json.dumps({'payload': payload_short, 'db_info': db_tag}, default=str))
            except Exception:
                # Fallback to logging if printing fails
                logging.info('sync_metadata upsert committed for sync_id=%s', sync_id)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
    except Exception as exc:
        # Emit a concise failure line to stdout for easier discovery in host logs
        try:
            import json as _json
            print('SYNC_METADATA_ERROR ' + _json.dumps({'sync_id': sync_id, 'error': str(exc)}))
        except Exception:
            pass
        logging.exception('Failed to persist sync metadata to DB: %s', exc)



def refresh_materialized_views():
    """Refresh materialized views used by the pipeline.

    When psycopg2 is not available, log the intent. When available, attempt to
    refresh views named in the environment variable `MATERIALIZED_VIEWS` (comma-separated),
    falling back to a default list if not supplied.
    """
    logging.info('refresh_materialized_views invoked')
    if not PSYCOPG_AVAILABLE:
        logging.info('psycopg2 not available; skipping materialized view refresh in local dry-run')
        return

    views = os.getenv('MATERIALIZED_VIEWS')
    if views:
        view_list = [v.strip() for v in views.split(',') if v.strip()]
    else:
        # sensible default (no-op if these don't exist)
        view_list = ['transport_documents_mv']

    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        for v in view_list:
            logging.info('Refreshing materialized view: %s', v)
            # Try CONCURRENTLY first
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {v}")
            except Exception:
                # Fallback to non-CONCURRENTLY
                logging.warning('Could not refresh concurrently, falling back to non-CONCURRENTLY for %s', v)
                cur.execute(f"REFRESH MATERIALIZED VIEW {v}")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logging.exception('Failed to refresh materialized views: %s', exc)
