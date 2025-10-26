import os
import sys
import json
from pathlib import Path
import argparse

import pandas as pd

# Local imports
sys.path.append(str(Path(__file__).resolve().parents[1]))
from helpers.parser import parse_ztdwr_file
from helpers.validator import validate_ztdwr_data
from helpers.transformer import transform_to_transport_documents
from helpers.env_loader import load_local_env


def main():
    parser = argparse.ArgumentParser(description='Smoke test parsing/validation/transformation for ZTDWR .dat')
    parser.add_argument('file', help='Path to .dat file to process')
    parser.add_argument('--to-db', action='store_true', help='Also attempt to upsert transformed data to the DB')
    parser.add_argument('--init-db', action='store_true', help='Initialize the local DB schema from sql/init_db.sql')
    args = parser.parse_args()

    # Load local .env if present
    load_local_env()

    file_path = args.file
    p = Path(file_path)
    if not p.exists():
        print(f"File not found: {file_path}")
        sys.exit(2)

    print(f"Reading: {p}")
    content = p.read_bytes()

    # Parse
    df = parse_ztdwr_file(content)
    print(f"Parsed rows={len(df)}, cols={len(df.columns)}")

    # Validate
    errors = validate_ztdwr_data(df)
    err_count = len(errors)
    rate = (err_count / len(df)) if len(df) else 0
    print(f"Validation errors={err_count} ({rate:.2%})")

    # Filter invalid rows if under threshold (as function does)
    threshold = float(os.getenv('VALIDATION_ERROR_THRESHOLD', '0.10'))
    if err_count > 0 and len(df) > 0 and rate <= threshold:
        invalid_rows = [e['row'] for e in errors if isinstance(e, dict) and 'row' in e]
        df = df.drop(index=invalid_rows)
        print(f"Continuing with valid rows: {len(df)} (dropped {len(invalid_rows)})")
    elif err_count > 0 and rate > threshold:
        print(f"ERROR: Validation error rate {rate:.2%} exceeds threshold {threshold:.2%}")
        # Still attempt transform to inspect issues

    # Transform
    transformed = transform_to_transport_documents(df, sync_id="local_smoke", file_name=p.name)
    print(f"Transformed rows={len(transformed)}, cols={len(transformed.columns)}")

    # Show a quick sample
    sample_cols = [
        'surat_pengantar_barang', 'waktu_timbang_terima', 'no_polisi',
        'nik_supir', 'bbm_actual', 'kilometer_actual', 'qty_dikirim', 'qty_diterima'
    ]
    present_cols = [c for c in sample_cols if c in transformed.columns]
    print("Sample rows (first 3):")
    with pd.option_context('display.max_columns', None, 'display.width', 200):
        print(transformed[present_cols].head(3))

    out = {
        'file': p.name,
        'rows_parsed': len(df),
        'validation_errors': err_count,
        'threshold': threshold,
        'transformed_cols': len(transformed.columns)
    }

    # Optionally initialize DB schema
    if args.init_db:
        try:
            import psycopg2  # quick check
        except Exception as e:
            print("psycopg2 is required to initialize the DB. Install requirements (use Python 3.11 for easier compatibility):")
            print("  python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt")
            sys.exit(3)

        try:
            from helpers.postgres_client import get_postgres_connection
            sql_file = Path(__file__).resolve().parents[1] / 'sql' / 'init_db.sql'
            if not sql_file.exists():
                print(f"Database init SQL not found: {sql_file}")
            else:
                sql = sql_file.read_text()
                conn = get_postgres_connection()
                cur = conn.cursor()
                cur.execute(sql)
                conn.commit()
                cur.close()
                conn.close()
                print("Database schema initialized from", sql_file)
        except Exception as e:
            print("Failed to initialize DB:", e)
            sys.exit(4)

    # Optionally upsert to DB
    if args.to_db:
        try:
            # Import DB helper lazily so psycopg2 isn't required for the simple parse/transform flow
            from helpers.postgres_client import upsert_to_postgres
        except Exception as e:
            print("psycopg2 or DB helper import failed. To enable DB upload, ensure requirements are installed and you run on Python 3.11:")
            print("  python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt")
            print("Error:", e)
            sys.exit(5)

        try:
            inserted, updated = upsert_to_postgres('local_smoke', transformed)
            out['db_inserted'] = inserted
            out['db_updated'] = updated
            print(f"DB upsert completed: inserted={inserted}, updated={updated}")
        except Exception as e:
            print("DB upsert failed:", e)
            sys.exit(6)

    print(json.dumps(out))


if __name__ == '__main__':
    main()
