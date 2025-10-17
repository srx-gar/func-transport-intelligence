"""Data validation utilities for ZTDWR data."""

import logging
from datetime import datetime

import pandas as pd


def _normalize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if normalized in {'', '~', 'NULL', 'null', 'None'}:
            return None
        return normalized
    return value


def _parse_decimal(value):
    normalized = _normalize_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, (int, float)):
        return float(normalized)
    text = str(normalized)
    if ',' in text and '.' in text:
        text = text.replace('.', '').replace(',', '.')
    elif ',' in text:
        text = text.replace(',', '.')
    try:
        return float(text)
    except ValueError:
        return None


def _combine_datetime(date_value, time_value=None):
    date_norm = _normalize_value(date_value)
    if date_norm is None:
        return None

    if isinstance(date_norm, (datetime, pd.Timestamp)):
        date_str = pd.to_datetime(date_norm).strftime('%Y-%m-%d')
    else:
        text = str(date_norm)
        if text.isdigit() and len(text) == 8:
            date_str = f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        else:
            parsed = pd.to_datetime(text, errors='coerce')
            if pd.isna(parsed):
                return None
            date_str = parsed.strftime('%Y-%m-%d')

    if time_value is None:
        return pd.to_datetime(date_str, errors='coerce')

    time_norm = _normalize_value(time_value)
    if time_norm is None:
        return pd.to_datetime(date_str, errors='coerce')

    if isinstance(time_norm, (datetime, pd.Timestamp)):
        time_str = pd.to_datetime(time_norm).strftime('%H:%M:%S')
    else:
        digits = str(time_norm).replace(':', '')
        if not digits.isdigit() or set(digits) == {'0'}:
            return pd.to_datetime(date_str, errors='coerce')
        if len(digits) == 6:
            time_str = f"{digits[0:2]}:{digits[2:4]}:{digits[4:6]}"
        elif len(digits) == 4:
            time_str = f"{digits[0:2]}:{digits[2:4]}:00"
        elif len(digits) == 2:
            time_str = f"{digits[0:2]}:00:00"
        else:
            return pd.to_datetime(date_str, errors='coerce')

    return pd.to_datetime(f"{date_str} {time_str}", errors='coerce')


def validate_ztdwr_data(df: pd.DataFrame) -> list:
    """Validate ZTDWR data quality."""

    errors = []

    required_columns = {
        'surat_pengantar_brg',
        'nik_supir',
        'no_polisi',
        'tanggal_timbang',
        'jam_timbang',
        'bbm_actual',
        'kilometer_actual',
    }

    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        errors.append({
            'type': 'MISSING_COLUMNS',
            'columns': sorted(missing_cols),
            'message': f"Missing required columns: {', '.join(sorted(missing_cols))}"
        })
        logging.error("Validation stopped due to missing columns: %s", missing_cols)
        return errors

    # Check for duplicate surat_pengantar_brg values
    duplicate_mask = df['surat_pengantar_brg'].duplicated(keep=False)
    if duplicate_mask.any():
        duplicates = df.loc[duplicate_mask, 'surat_pengantar_brg'].tolist()
        errors.append({
            'type': 'DUPLICATE_SURAT_PENGANTAR',
            'spb_ids': duplicates,
            'message': 'Duplicate surat_pengantar_brg values detected'
        })

    # Row-level validations
    for idx, row in df.iterrows():
        row_errors = []

        spb_id = _normalize_value(row.get('surat_pengantar_brg'))
        if spb_id is None:
            row_errors.append('surat_pengantar_brg is null or empty')

        bbm = _parse_decimal(row.get('bbm_actual'))
        km = _parse_decimal(row.get('kilometer_actual'))

        if bbm is None or not (0 < bbm <= 500):
            row_errors.append(f'bbm_actual invalid: {row.get("bbm_actual")!r}')

        if km is None or not (0 < km <= 500):
            row_errors.append(f'kilometer_actual invalid: {row.get("kilometer_actual")!r}')

        timestamp = _combine_datetime(row.get('tanggal_timbang'), row.get('jam_timbang'))
        if pd.isna(timestamp):
            row_errors.append('tanggal_timbang/jam_timbang invalid')

        driver_nik = _normalize_value(row.get('nik_supir'))
        if driver_nik is None:
            row_errors.append('nik_supir is null or empty')

        no_polisi = _normalize_value(row.get('no_polisi'))
        if no_polisi is None:
            row_errors.append('no_polisi is null or empty')

        if row_errors:
            errors.append({
                'row': idx,
                'surat_pengantar_brg': spb_id or 'UNKNOWN',
                'errors': row_errors
            })

    logging.info("Validation complete: %s errors found in %s rows", len(errors), len(df))
    return errors
