"""Data transformation utilities for ZTDWR to transport_documents schema."""

import logging
from datetime import datetime, timezone
from typing import Optional

import pandas as pd


TARGET_COLUMNS = [
    'surat_pengantar_barang',
    'waktu_buat',
    'no_tiket',
    'transporter',
    'transporter_name',
    'sender',
    'sender_name',
    'receiver',
    'receiver_name',
    'nama_pt',
    'pengangkut',
    'id_kendaraan',
    'jenis_kendaraan',
    'kapasitas_kendaraan',
    'category_truck',
    'jenis_angkutan',
    'nomor_asset',
    'qty_dikirim',
    'qty_diterima',
    'base_unit_of_measure',
    'waktu_surat_jalan',
    'waktu_timbang_kirim',
    'waktu_timbang_terima',
    'waktu_kembali',
    'no_kontrak_do',
    'equipment',
    'no_polisi',
    'kilometer_std',
    'kilometer_actual',
    'bbm_std',
    'bbm_actual',
    'asal',
    'tujuan',
    'ongkos_angkut',
    'divisi_sender',
    'division_receiver',
    'external',
    'reservation',
    'item_no_stock_transfer_reserv',
    'nik_supir',
    'nik_pemuat_1',
    'nik_pemuat_2',
    'nik_pemuat_3',
    'nik_pemuat_4',
    'nik_pemuat_5',
    'nik_supir_mandah',
    'nama_supir',
    'hk',
    'waktu_pembuatan_konfirmasi',
    'user_pembuatan_konfirmasi',
    'waktu_pembatalan_konfirmasi',
    'user_pembatalan_konfirmasi',
    'susut_timbang',
    'pindah_muatan',
    'jaring_tbs',
    'settlement_order',
    'kontanan',
    'sewa',
    'flag_post_yes',
    'backhauling',
    'posisi_divisi',
    'nomor_work_order',
    'no_work_order_operation',
    'no_konfirmasi',
    'konfirmasi_counter',
    'konfirmasi_posting_date',
    'posting_date_gi',
    'good_issue_document',
    'measurement_document',
    'no_po',
    'no_so_besar',
    'so_besar_item',
    'no_so_kecil',
    'so_kecil_item',
    'waktu_pembuatan_so_kecil',
    'user_pembuatan_so_kecil',
    'delete_by',
    'delete_on',
    'deletion_indicator_in_tdw',
    'processed_gi',
    'cancel_gi',
    'processed_confirmation',
    'changed_by',
    'changed_date',
    'ztdw_enhancement',
    'changed_time',
]

TRUE_VALUES = {'X', '1', 'Y', 'YES', 'TRUE', 'T'}
FALSE_VALUES = {'0', 'N', 'NO', 'FALSE', 'F'}


def _normalize_value(value):
    """Standardize placeholder strings to None and strip whitespace."""
    if pd.isna(value):
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if normalized in {'', '~', 'NULL', 'null', 'None'}:
            return None
        return normalized
    return value


def _string_value(value):
    normalized = _normalize_value(value)
    return str(normalized) if normalized is not None else None


def _decimal_value(value):
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


def _int_value(value):
    normalized = _normalize_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, int):
        return normalized
    # Convert from decimal-like strings safely
    dec = _decimal_value(normalized)
    if dec is None:
        return None
    try:
        return int(round(dec))
    except Exception:
        return None


def _bool_value(value):
    normalized = _normalize_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, bool):
        return normalized
    text = str(normalized).strip().upper()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return None


def _normalize_date_component(value):
    normalized = _normalize_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, (pd.Timestamp, datetime)):
        return pd.to_datetime(normalized).strftime('%Y-%m-%d')

    text = str(normalized)
    if text.isdigit() and len(text) == 8:
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"

    parsed = pd.to_datetime(text, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.strftime('%Y-%m-%d')


def _normalize_time_component(value):
    normalized = _normalize_value(value)
    if normalized is None:
        return None
    if isinstance(normalized, (pd.Timestamp, datetime)):
        return pd.to_datetime(normalized).strftime('%H:%M:%S')

    text = str(normalized).replace(':', '')
    if not text.isdigit():
        return None
    if set(text) == {'0'}:
        return None

    length = len(text)
    if length == 6:
        return f"{text[0:2]}:{text[2:4]}:{text[4:6]}"
    if length == 4:
        return f"{text[0:2]}:{text[2:4]}:00"
    if length == 2:
        return f"{text[0:2]}:00:00"
    return None


def _combine_datetime_series(df: pd.DataFrame, date_col: str, time_col: Optional[str] = None) -> pd.Series:
    """Optimized datetime combination using vectorized operations."""
    if date_col not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index)

    # Vectorized date parsing
    date_series = df[date_col].astype(str).str.strip()

    # Handle YYYYMMDD format (8 digits)
    date_formatted = date_series.copy()
    is_8digit = date_series.str.match(r'^\d{8}$')
    if is_8digit.any():
        date_formatted[is_8digit] = (
            date_series[is_8digit].str[:4] + '-' +
            date_series[is_8digit].str[4:6] + '-' +
            date_series[is_8digit].str[6:8]
        )

    # Parse dates
    dates = pd.to_datetime(date_formatted, errors='coerce')

    # If no time column, return dates
    if not time_col or time_col not in df.columns:
        return dates

    # Vectorized time parsing
    time_series = df[time_col].astype(str).str.strip().str.replace(':', '')

    # Format time strings
    time_formatted = pd.Series(['00:00:00'] * len(df), index=df.index)

    # 6 digits: HHMMSS
    is_6digit = time_series.str.match(r'^\d{6}$') & (time_series != '000000')
    if is_6digit.any():
        time_formatted[is_6digit] = (
            time_series[is_6digit].str[:2] + ':' +
            time_series[is_6digit].str[2:4] + ':' +
            time_series[is_6digit].str[4:6]
        )

    # 4 digits: HHMM
    is_4digit = time_series.str.match(r'^\d{4}$')
    if is_4digit.any():
        time_formatted[is_4digit] = (
            time_series[is_4digit].str[:2] + ':' +
            time_series[is_4digit].str[2:4] + ':00'
        )

    # 2 digits: HH
    is_2digit = time_series.str.match(r'^\d{2}$')
    if is_2digit.any():
        time_formatted[is_2digit] = time_series[is_2digit] + ':00:00'

    # Combine date and time
    datetime_strings = dates.dt.strftime('%Y-%m-%d') + ' ' + time_formatted
    result = pd.to_datetime(datetime_strings, errors='coerce')

    # If time was invalid or all zeros, use date only
    result[dates.notna() & result.isna()] = dates[dates.notna() & result.isna()]

    return result


def _get_series(df: pd.DataFrame, column: str, converter, default=None) -> pd.Series:
    """Optimized series getter with vectorized operations where possible."""
    if column not in df.columns:
        return pd.Series([default] * len(df), index=df.index)

    series = df[column]

    # Use vectorized operations for common converters
    if converter == _string_value:
        return _vectorized_string_series(series)
    elif converter == _decimal_value:
        return _vectorized_decimal_series(series)
    elif converter == _int_value:
        return _vectorized_int_series(series)
    elif converter == _bool_value:
        return _vectorized_bool_series(series)
    else:
        # Fall back to apply for custom converters
        return series.apply(converter)


def _vectorized_string_series(series: pd.Series) -> pd.Series:
    """Fast vectorized string normalization."""
    # Replace empty strings and placeholders with None
    result = series.astype(str).str.strip()
    result = result.replace(['', '~', 'NULL', 'null', 'None', 'nan', 'NaN', 'NAN', '<NA>'], None)
    return result


def _vectorized_primary_key_series(series: pd.Series) -> pd.Series:
    """
    Special handler for primary key column - converts NULL/empty values to None for filtering.
    Strips delimiters and whitespace, marks invalid values as None to be filtered out later.
    """
    # Start with None for all NaN/NA values in the original series
    result = pd.Series([None] * len(series), index=series.index, dtype=object)

    # Identify non-null values in the original series
    valid_mask = series.notna()

    if valid_mask.any():
        # Convert valid values to string and clean them
        cleaned = series[valid_mask].astype(str).str.strip()

        # Remove common delimiters
        cleaned = cleaned.str.lstrip('^').str.rstrip('~').str.strip()

        # Create mask for invalid string values (comprehensive list)
        invalid_strings = cleaned.isin([
            '', '~', 'NULL', 'null', 'None', 'none',
            'nan', 'NaN', 'NAN', '<NA>', '<na>',
            'nat', 'NaT', 'NAT',  # datetime NA values
            'pd.NA', 'pd.NaT'  # pandas NA representations
        ])

        # Only keep values that are not invalid
        result[valid_mask & ~invalid_strings] = cleaned[~invalid_strings]
        # result remains None for invalid strings

    return result


def _vectorized_decimal_series(series: pd.Series) -> pd.Series:
    """Fast vectorized decimal conversion."""
    # Try direct numeric conversion first
    result = pd.to_numeric(series, errors='coerce')

    # Handle string decimals with comma separators
    mask = result.isna() & series.notna()
    if mask.any():
        # Convert strings with commas to decimals
        str_series = series[mask].astype(str).str.strip()
        # Replace European format (1.234,56 -> 1234.56)
        str_series = str_series.str.replace(r'\.(?=\d{3})', '', regex=True)
        str_series = str_series.str.replace(',', '.')
        result[mask] = pd.to_numeric(str_series, errors='coerce')

    return result


def _vectorized_int_series(series: pd.Series) -> pd.Series:
    """Fast vectorized integer conversion."""
    # Convert to decimal first, then round to int
    decimals = _vectorized_decimal_series(series)
    return decimals.round().astype('Int64')  # Nullable integer type


def _vectorized_bool_series(series: pd.Series) -> pd.Series:
    """Fast vectorized boolean conversion."""
    # Convert to uppercase strings
    str_series = series.astype(str).str.strip().str.upper()

    # Map true values
    result = pd.Series([None] * len(series), index=series.index, dtype='object')
    true_mask = str_series.isin(TRUE_VALUES)
    false_mask = str_series.isin(FALSE_VALUES)

    result[true_mask] = True
    result[false_mask] = False

    return result


def transform_to_transport_documents(
    df: pd.DataFrame,
    sync_id: str,
    file_name: str
) -> pd.DataFrame:
    """Transform ZTDWR DataFrame to transport_documents schema."""
    import time

    start_time = time.time()
    logging.info("Starting transformation to transport_documents schema (%d rows)", len(df))

    transformed = pd.DataFrame(index=df.index)

    # CRITICAL: Use special handler for primary key to prevent NULL conversion
    transformed['surat_pengantar_barang'] = _vectorized_primary_key_series(df['surat_pengantar_brg']) if 'surat_pengantar_brg' in df.columns else pd.Series([None] * len(df), index=df.index)

    # Direct mappings with optional conversions
    transformed['waktu_buat'] = _combine_datetime_series(df, 'tanggal_buat', 'waktu_buat')
    transformed['no_tiket'] = _get_series(df, 'no_tiket', _string_value)
    transformed['transporter'] = _get_series(df, 'transporter', _string_value)
    transformed['transporter_name'] = _get_series(df, 'transporter_name', _string_value)
    transformed['sender'] = _get_series(df, 'sender', _string_value)
    transformed['sender_name'] = _get_series(df, 'sender_name', _string_value)
    transformed['receiver'] = _get_series(df, 'receiver', _string_value)
    transformed['receiver_name'] = _get_series(df, 'receiver_name', _string_value)
    transformed['nama_pt'] = _get_series(df, 'nama_pt', _string_value)
    transformed['pengangkut'] = _get_series(df, 'pengangkut', _string_value)
    transformed['id_kendaraan'] = _get_series(df, 'id_kendaraan', _string_value)
    transformed['jenis_kendaraan'] = _get_series(df, 'jenis_kendaraan', _string_value)
    transformed['kapasitas_kendaraan'] = _get_series(df, 'kapasitas_kendaraan', _decimal_value)
    transformed['category_truck'] = _get_series(df, 'type_equipment', _string_value)
    transformed['jenis_angkutan'] = _get_series(df, 'jenis_angkutan', _string_value)
    transformed['nomor_asset'] = _get_series(df, 'nomor_aset', _string_value)
    transformed['qty_dikirim'] = _get_series(df, 'qty_dikirim', _decimal_value)
    transformed['qty_diterima'] = _get_series(df, 'qty_diterima', _decimal_value)
    transformed['base_unit_of_measure'] = _get_series(df, 'base_unit_of_measure', _string_value)
    transformed['waktu_surat_jalan'] = _combine_datetime_series(df, 'tanggal_surat_jalan', 'jam_surat_jalan')
    transformed['waktu_timbang_kirim'] = _combine_datetime_series(df, 'tanggal_timbang_kirim', 'jam_timbang_kirim')
    transformed['waktu_timbang_terima'] = _combine_datetime_series(df, 'tanggal_timbang', 'jam_timbang')
    transformed['waktu_kembali'] = _combine_datetime_series(df, 'tanggal_kembali', 'jam_kembali')
    transformed['no_kontrak_do'] = _get_series(df, 'no_kontrak', _string_value)
    transformed['equipment'] = _get_series(df, 'equipment', _string_value)
    transformed['no_polisi'] = _get_series(df, 'no_polisi', _string_value)
    transformed['kilometer_std'] = _get_series(df, 'kilometer_std', _decimal_value)
    transformed['kilometer_actual'] = _get_series(df, 'kilometer_actual', _decimal_value)
    transformed['bbm_std'] = _get_series(df, 'bbm_std', _decimal_value)
    transformed['bbm_actual'] = _get_series(df, 'bbm_actual', _decimal_value)
    transformed['asal'] = _get_series(df, 'asal', _string_value)
    transformed['tujuan'] = _get_series(df, 'tujuan', _string_value)
    transformed['ongkos_angkut'] = _get_series(df, 'ongkos_angkut', _decimal_value)
    transformed['divisi_sender'] = _get_series(df, 'divisi_sender', _string_value)
    transformed['division_receiver'] = _get_series(df, 'division_receiver', _string_value)
    transformed['external'] = _get_series(df, 'client', _string_value)
    transformed['reservation'] = _get_series(df, 'reservation', _string_value)
    transformed['item_no_stock_transfer_reserv'] = _get_series(df, 'item_no_stock_transfer_reserv', _string_value)
    transformed['nik_supir'] = _get_series(df, 'nik_supir', _string_value)
    transformed['nik_pemuat_1'] = _get_series(df, 'nik_pemuat_1', _string_value)
    transformed['nik_pemuat_2'] = _get_series(df, 'nik_pemuat_2', _string_value)
    transformed['nik_pemuat_3'] = _get_series(df, 'nik_pemuat_3', _string_value)
    transformed['nik_pemuat_4'] = _get_series(df, 'nik_pemuat_4', _string_value)
    transformed['nik_pemuat_5'] = _get_series(df, 'nik_pemuat_5', _string_value)
    transformed['nik_supir_mandah'] = _get_series(df, 'nik_supir_mandah', _string_value)
    transformed['nama_supir'] = _get_series(df, 'nama_supir', _string_value)
    transformed['hk'] = _get_series(df, 'hk', _decimal_value)
    transformed['waktu_pembuatan_konfirmasi'] = _combine_datetime_series(df, 'tanggal_pembuatan_konfirmasi', 'jam_pembuatan_konfirmasi')
    transformed['user_pembuatan_konfirmasi'] = _get_series(df, 'user_pembuatan_konfirmasi', _string_value)
    transformed['waktu_pembatalan_konfirmasi'] = _combine_datetime_series(df, 'tanggal_pembatalan_konfirmasi', 'jam_pembatalan_konfirmasi')
    transformed['user_pembatalan_konfirmasi'] = _get_series(df, 'user_pembatalan_konfirmasi', _string_value)
    transformed['susut_timbang'] = _get_series(df, 'susut_timbang', _bool_value)
    transformed['pindah_muatan'] = _get_series(df, 'pindah_muatan', _string_value)
    transformed['jaring_tbs'] = _get_series(df, 'jaring_tbs', _decimal_value)
    transformed['settlement_order'] = _get_series(df, 'settlement_order', _string_value)
    transformed['kontanan'] = _get_series(df, 'kontanan', _bool_value)
    transformed['sewa'] = _get_series(df, 'sewa', _bool_value)
    transformed['flag_post_yes'] = _get_series(df, 'flag_post', _bool_value)
    transformed['backhauling'] = _get_series(df, 'backhauling', _string_value)
    transformed['posisi_divisi'] = _get_series(df, 'posisi_divisi', _string_value)
    transformed['nomor_work_order'] = _get_series(df, 'nomor_work_order', _string_value)
    transformed['no_work_order_operation'] = _get_series(df, 'no_work_order_operation', _string_value)
    transformed['no_konfirmasi'] = _get_series(df, 'no_konfirmasi', _string_value)
    transformed['konfirmasi_counter'] = _get_series(df, 'konfirmasi_counter', _int_value)
    transformed['konfirmasi_posting_date'] = _combine_datetime_series(df, 'konfirmasi_posting_date')
    transformed['posting_date_gi'] = _combine_datetime_series(df, 'posting_date_gi')
    transformed['good_issue_document'] = _get_series(df, 'good_issue_document', _string_value)
    transformed['measurement_document'] = _get_series(df, 'measurement_document', _string_value)
    transformed['no_po'] = _get_series(df, 'no_po', _string_value)
    transformed['no_so_besar'] = _get_series(df, 'no_so_besar', _string_value)
    transformed['so_besar_item'] = _get_series(df, 'so_besar_item', _string_value)
    transformed['no_so_kecil'] = _get_series(df, 'no_so_kecil', _string_value)
    transformed['so_kecil_item'] = _get_series(df, 'so_kecil_item', _string_value)
    transformed['waktu_pembuatan_so_kecil'] = _combine_datetime_series(df, 'tanggal_pembuatan_so_kecil', 'jam_pembuatan_so_kecil')
    transformed['user_pembuatan_so_kecil'] = _get_series(df, 'user_pembuatan_so_kecil', _string_value)
    transformed['delete_by'] = _get_series(df, 'delete_by', _string_value)
    transformed['delete_on'] = _combine_datetime_series(df, 'delete_on')
    transformed['deletion_indicator_in_tdw'] = _get_series(df, 'delete_indicator_in_tdw', _string_value)
    transformed['processed_gi'] = _get_series(df, 'processed_gi', _bool_value)
    transformed['cancel_gi'] = _get_series(df, 'cancel_gi', _bool_value)
    transformed['processed_confirmation'] = _get_series(df, 'processed_confirmation', _bool_value)
    transformed['changed_by'] = _get_series(df, 'change_by', _string_value)
    transformed['changed_date'] = _combine_datetime_series(df, 'change_date')
    transformed['ztdw_enhancement'] = _get_series(df, 'ztdw_enhancement', _bool_value)
    transformed['changed_time'] = _get_series(df, 'change_time', _string_value)

    # CRITICAL: Check for NULL primary keys BEFORE reordering columns
    # This catches issues early and logs the exact source
    initial_count = len(transformed)
    pk_series_early = transformed['surat_pengantar_barang']
    null_pk_mask_early = (
        pk_series_early.isna() |
        (pk_series_early == '') |
        (pk_series_early == 'None') |
        (pk_series_early == 'nan') |
        (pk_series_early == 'NaN') |
        (pk_series_early == 'NAN') |
        (pk_series_early == '<NA>') |
        (pk_series_early.astype(str).str.strip() == '') |
        (pk_series_early.astype(str).str.strip() == 'None') |
        (pk_series_early.astype(str).str.strip() == 'nan') |
        (pk_series_early.astype(str).str.strip() == '<NA>')
    )
    null_pk_count_early = null_pk_mask_early.sum()

    if null_pk_count_early > 0:
        logging.warning(
            "⚠️  BEFORE REORDERING: Found %d rows with NULL/empty primary key - will filter after adding audit columns",
            null_pk_count_early
        )
        # Log sample of original source values for these rows
        null_indices = transformed[null_pk_mask_early].index[:5].tolist()
        for idx in null_indices[:3]:
            orig_val = df.loc[idx, 'surat_pengantar_brg'] if 'surat_pengantar_brg' in df.columns and idx in df.index else 'N/A'
            trans_val = transformed.loc[idx, 'surat_pengantar_barang']
            logging.warning(f"  Row {idx}: source='{orig_val}' → transformed='{trans_val}' (type: {type(trans_val)})")

    # Ensure all expected columns exist even if missing from source
    for column in TARGET_COLUMNS:
        if column not in transformed.columns:
            transformed[column] = pd.Series([None] * len(df), index=df.index)

    # Order columns consistently
    transformed = transformed[TARGET_COLUMNS]

    # Add audit columns required by downstream upsert
    timestamp_now = datetime.now(timezone.utc)
    transformed['created_at'] = timestamp_now
    transformed['updated_at'] = timestamp_now
    transformed['sync_id'] = sync_id
    transformed['source_file'] = file_name

    # FINAL: Filter out rows with NULL primary key to prevent database constraint violations
    # Re-check after reordering in case something changed
    # Check for: None, NaN, empty string, 'None' string, 'nan' string, '<NA>', pd.NA
    pk_series = transformed['surat_pengantar_barang']
    null_pk_mask = (
        pk_series.isna() |
        (pk_series == '') |
        (pk_series == 'None') |
        (pk_series == 'nan') |
        (pk_series == 'NaN') |
        (pk_series == 'NAN') |
        (pk_series == '<NA>') |
        (pk_series.astype(str).str.strip() == '') |
        (pk_series.astype(str).str.strip() == 'None') |
        (pk_series.astype(str).str.strip() == 'nan') |
        (pk_series.astype(str).str.strip() == '<NA>')
    )
    null_pk_count = null_pk_mask.sum()

    if null_pk_count > 0:
        logging.warning(
            "⚠️  Found %d rows with NULL/empty primary key (surat_pengantar_barang) after transformation - filtering them out",
            null_pk_count
        )
        # Show sample of NULL PK rows for debugging
        null_pk_indices = transformed[null_pk_mask].index[:5].tolist()
        logging.warning(
            "Sample NULL PK row indices: %s (check source data at these positions)",
            null_pk_indices
        )
        # Log the actual values for debugging
        for idx in null_pk_indices[:3]:
            pk_val = transformed.loc[idx, 'surat_pengantar_barang']
            logging.warning(f"  Row {idx}: surat_pengantar_barang='{pk_val}' (type: {type(pk_val)})")

        transformed = transformed[~null_pk_mask].copy()
        logging.warning(
            "Reduced from %d to %d rows after filtering NULL primary keys",
            initial_count,
            len(transformed)
        )

    elapsed = time.time() - start_time
    rows_per_sec = initial_count / elapsed if elapsed > 0 else 0
    logging.info(
        "✅ Transformed %d records in %.2f seconds (%.0f rows/sec)",
        len(transformed),
        elapsed,
        rows_per_sec
    )

    return transformed
