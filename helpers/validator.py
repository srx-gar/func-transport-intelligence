"""
Data validation utilities for ZTDWR data
"""

import pandas as pd
import logging


def validate_ztdwr_data(df: pd.DataFrame) -> list:
    """
    Validate ZTDWR data quality.

    Returns list of validation errors with row numbers.
    """
    errors = []

    # Required columns check
    required_columns = [
        'SPB_ID', 'DRIVER_NIK', 'NOPOL', 'WAKTU_TIMBANG_TERIMA',
        'BBM_ACTUAL', 'KILOMETER_ACTUAL'
    ]

    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        errors.append({
            'type': 'MISSING_COLUMNS',
            'columns': list(missing_cols),
            'message': f"Missing required columns: {', '.join(missing_cols)}"
        })
        return errors  # Cannot proceed without required columns

    # Row-level validations
    for idx, row in df.iterrows():
        row_errors = []

        # SPB_ID must be unique and non-null
        if pd.isna(row['SPB_ID']) or str(row['SPB_ID']).strip() == '':
            row_errors.append('SPB_ID is null or empty')

        # Fuel and distance validation (business rule from BRD)
        try:
            bbm = float(row['BBM_ACTUAL'])
            km = float(row['KILOMETER_ACTUAL'])

            if not (0 < bbm <= 500):
                row_errors.append(f'BBM_ACTUAL out of range (0-500): {bbm}')
            if not (0 < km <= 500):
                row_errors.append(f'KILOMETER_ACTUAL out of range (0-500): {km}')
        except (ValueError, TypeError):
            row_errors.append('BBM_ACTUAL or KILOMETER_ACTUAL not numeric')

        # Date validation
        try:
            pd.to_datetime(row['WAKTU_TIMBANG_TERIMA'])
        except:
            row_errors.append(
                f'Invalid date format: {row["WAKTU_TIMBANG_TERIMA"]}'
            )

        # Driver NIK validation
        if pd.isna(row['DRIVER_NIK']) or str(row['DRIVER_NIK']).strip() == '':
            row_errors.append('DRIVER_NIK is null or empty')

        # Nopol (vehicle plate) validation
        if pd.isna(row['NOPOL']) or str(row['NOPOL']).strip() == '':
            row_errors.append('NOPOL is null or empty')

        if row_errors:
            errors.append({
                'row': idx,
                'spb_id': str(row.get('SPB_ID', 'UNKNOWN')),
                'errors': row_errors
            })

    logging.info(f"Validation complete: {len(errors)} errors found in {len(df)} rows")
    return errors
