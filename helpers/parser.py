"""
File parsing utilities for ZTDWR .dat files
"""

import io
import pandas as pd
import logging


def is_gzipped(content: bytes) -> bool:
    """
    Check if content is gzipped by checking magic number.
    Gzip files start with 0x1f 0x8b
    """
    return len(content) >= 2 and content[:2] == b'\x1f\x8b'


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

    # Detect delimiter (tab or pipe)
    first_line = text.split('\n')[0]
    if '\t' in first_line:
        delimiter = '\t'
    elif '|' in first_line:
        delimiter = '|'
    else:
        # Default to tab
        delimiter = '\t'

    logging.info(f"Detected delimiter: {repr(delimiter)}")

    # Parse with pandas
    df = pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        dtype=str,  # Read all as string initially
        na_values=['', 'NULL', 'null', 'None'],
        keep_default_na=False
    )

    # Strip whitespace from all string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    logging.info(f"Parsed {len(df)} rows, {len(df.columns)} columns")
    logging.info(f"Columns: {list(df.columns)}")

    return df
