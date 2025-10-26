"""
File parsing utilities for ZTDWR .dat files
"""

import io
import pandas as pd
import logging
import re


def is_gzipped(content: bytes) -> bool:
    """
    Check if content is gzipped by checking magic number.
    Gzip files start with 0x1f 0x8b
    """
    return len(content) >= 2 and content[:2] == b'\x1f\x8b'


def _canonicalize_column_name(col: str) -> str:
    """Convert a raw header into a normalized lowercase snake_case token.

    Examples:
    - 'SPB_ID' -> 'spb_id'
    - 'No Polisi' -> 'no_polisi'
    - 'WAKTU_TIMBANG_TERIMA' -> 'waktu_timbang_terima'
    """
    if col is None:
        return ''
    # strip surrounding whitespace and control characters
    col = str(col).strip()
    # Replace sequences of non-alphanumeric characters with underscore
    col = re.sub(r"[^0-9a-zA-Z]+", "_", col)
    # Collapse multiple underscores
    col = re.sub(r"_+", "_", col)
    return col.strip("_").lower()


def parse_ztdwr_file(content: bytes) -> pd.DataFrame:
    """
    Parse ZTDWR .dat file to DataFrame.

    Format assumptions:
    - Tab-separated values (TSV) or pipe-separated
    - First row contains headers
    - Encoding: UTF-8 or Latin-1
    """
    try:
        # Try UTF-8 first
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        # Fallback to Latin-1
        logging.warning("UTF-8 decode failed, falling back to Latin-1")
        text = content.decode('latin-1')

    # Detect delimiter (supports multi-character custom delimiters)
    first_line = text.split('\n')[0]
    if '\t' in first_line and '~|^' not in first_line:
        delimiter = '\t'
        engine = 'c'
    elif '~|^' in first_line:
        delimiter = r'~\|\^'
        engine = 'python'  # regex separator requires Python engine
    elif '|' in first_line:
        delimiter = '|'
        engine = 'c'
    else:
        delimiter = '\t'
        engine = 'c'

    logging.info(f"Detected delimiter: {repr(delimiter)} (engine={engine})")

    # Parse with pandas
    df = pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        engine=engine,
        dtype=str,  # Read all as string initially
        na_values=['', 'NULL', 'null', 'None', '~'],
        keep_default_na=False
    )

    # Strip whitespace from all string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Normalize column names to lowercase snake_case for downstream processing
    # First perform a best-effort canonicalization of raw headers
    df.columns = [_canonicalize_column_name(col) for col in df.columns]

    # Map common alternative headers to canonical names expected by validator/transformer
    canonical_map = {
        # SPB / surat pengantar
        'spb_id': 'surat_pengantar_brg',
        'spb': 'surat_pengantar_brg',
        'surat_pengantar': 'surat_pengantar_brg',
        'surat_pengantar_brg': 'surat_pengantar_brg',

        # Driver / NIK
        'driver_nik': 'nik_supir',
        'driverid': 'nik_supir',
        'nik_supir': 'nik_supir',
        'niksupir': 'nik_supir',

        # No polisi variants
        'nopol': 'no_polisi',
        'no_pol': 'no_polisi',
        'no_polisi': 'no_polisi',

        # Kilometer and BBM
        'kilometer_actual': 'kilometer_actual',
        'kilometer_act': 'kilometer_actual',
        'kilometer': 'kilometer_actual',
        'bbm_actual': 'bbm_actual',
        'bbm': 'bbm_actual',

        # Waktu / tanggal timbang (accept several naming schemes)
        'waktu_timbang_terima': 'jam_timbang',
        'waktu_timbang': 'jam_timbang',
        'jam_timbang': 'jam_timbang',
        'tanggal_timbang': 'tanggal_timbang',
        'tanggal_timbang_terima': 'tanggal_timbang',
        'tanggal_tim': 'tanggal_timbang',
    }

    # Apply mapping where possible; otherwise keep the canonicalized name
    new_cols = []
    for col in df.columns:
        mapped = canonical_map.get(col, col)
        new_cols.append(mapped)
    df.columns = new_cols

    logging.info(f"Parsed {len(df)} rows, {len(df.columns)} columns")
    logging.info(f"Columns: {list(df.columns)}")

    return df
