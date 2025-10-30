"""
Streaming file parser for large ZTDWR .dat files.
Processes files in chunks to minimize memory usage.
"""

import io
import gzip
import pandas as pd
import logging
import re
from typing import Iterator, Optional, Tuple


def _canonicalize_column_name(col: str) -> str:
    """Convert a raw header into a normalized lowercase snake_case token."""
    if col is None:
        return ''
    col = str(col)
    col = col.replace('\ufeff', '')
    col = col.strip()
    col = re.sub(r"[^0-9a-zA-Z]+", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_").lower()


def _detect_delimiter(first_line: str) -> Tuple[str, str]:
    """Detect the delimiter and pandas engine to use."""
    normalized_first_line = first_line.replace('\\', '')

    if '\t' in first_line and '~^' not in normalized_first_line:
        return '\t', 'c'
    elif '~^' in normalized_first_line:
        return r'~\\^', 'python'
    elif '' in first_line:
        return '', 'c'
    else:
        return '\t', 'c'


def _sanitize_header_line(header_bytes: bytes) -> bytes:
    """Remove stray delimiter artifacts from header."""
    header_tokens = header_bytes.split(b"")
    cleaned_tokens = []
    for t in header_tokens:
        s = t.replace(b"\\", b"").strip()
        while s.startswith(b"^"):
            s = s[1:]
        while s.endswith(b"~"):
            s = s[:-1]
        cleaned_tokens.append(s)
    return b"".join(cleaned_tokens)


def parse_ztdwr_chunks(
    content: bytes,
    chunk_size: int = 10000,
    decompress: bool = True
) -> Iterator[pd.DataFrame]:
    """
    Parse ZTDWR file in chunks to minimize memory usage.

    Args:
        content: Raw file content (may be gzipped)
        chunk_size: Number of rows per chunk
        decompress: Whether to decompress if gzipped

    Yields:
        DataFrame chunks with normalized column names
    """
    # Decompress if needed
    if decompress and len(content) >= 2 and content[:2] == b'\x1f\x8b':
        logging.info("Decompressing payload from %s bytes", len(content))
        content = gzip.decompress(content)
        logging.info("Decompressed to %s bytes", len(content))

    # Sanitize header
    parts = content.split(b"\n", 1)
    header_bytes = parts[0]
    rest = parts[1] if len(parts) > 1 else b''

    sanitized_header = _sanitize_header_line(header_bytes)
    content = sanitized_header + b"\n" + rest

    # Decode content
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        logging.warning("UTF-8 decode failed, falling back to Latin-1")
        text = content.decode('latin-1')

    # Detect delimiter
    first_line = text.split('\n')[0]
    delimiter, engine = _detect_delimiter(first_line)
    logging.info(f"Detected delimiter: {repr(delimiter)} (engine={engine})")

    # Parse headers to get column mapping
    header_df = pd.read_csv(
        io.StringIO(first_line),
        sep=delimiter,
        engine=engine,
        nrows=0
    )

    # Clean and normalize headers
    raw_headers = list(header_df.columns)
    cleaned_headers = []
    for h in raw_headers:
        s = str(h).replace('\\', '').strip()
        s = s.lstrip('^').rstrip('~')
        cleaned_headers.append(s)

    # Canonicalize and map to standard names
    canonical_headers = [_canonicalize_column_name(col) for col in cleaned_headers]

    canonical_map = {
        'spb_id': 'surat_pengantar_brg',
        'spb': 'surat_pengantar_brg',
        'surat_pengantar': 'surat_pengantar_brg',
        'surat_pengantar_brg': 'surat_pengantar_brg',
        'driver_nik': 'nik_supir',
        'driverid': 'nik_supir',
        'nik_supir': 'nik_supir',
        'niksupir': 'nik_supir',
        'nopol': 'no_polisi',
        'no_pol': 'no_polisi',
        'no_polisi': 'no_polisi',
        'kilometer_actual': 'kilometer_actual',
        'kilometer_act': 'kilometer_actual',
        'kilometer': 'kilometer_actual',
        'bbm_actual': 'bbm_actual',
        'bbm': 'bbm_actual',
        'waktu_timbang_terima': 'jam_timbang',
        'waktu_timbang': 'jam_timbang',
        'jam_timbang': 'jam_timbang',
        'tanggal_timbang': 'tanggal_timbang',
        'tanggal_timbang_terima': 'tanggal_timbang',
        'tanggal_tim': 'tanggal_timbang',
    }

    mapped_headers = [canonical_map.get(col, col) for col in canonical_headers]

    # Read file in chunks
    text_io = io.StringIO(text)
    chunk_iter = pd.read_csv(
        text_io,
        sep=delimiter,
        engine=engine,
        dtype=str,
        na_values=['', 'NULL', 'null', 'None', '~'],
        keep_default_na=False,
        chunksize=chunk_size
    )

    chunk_num = 0
    for chunk_df in chunk_iter:
        chunk_num += 1
        # Apply header mapping
        chunk_df.columns = mapped_headers

        # Strip whitespace
        chunk_df = chunk_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        logging.info(f"📦 Yielding chunk #{chunk_num} with {len(chunk_df)} rows")
        yield chunk_df

    logging.info(f"✅ Finished parsing all {chunk_num} chunks")


def estimate_chunk_size(file_size_bytes: int, available_memory_mb: int = 512) -> int:
    """
    Estimate optimal chunk size based on file size and available memory.

    Args:
        file_size_bytes: Size of the file in bytes
        available_memory_mb: Available memory in MB (default: 512MB)

    Returns:
        Suggested chunk size (number of rows)
    """
    # Assume each row takes ~2KB in memory (conservative estimate)
    bytes_per_row = 2048

    # Use at most 50% of available memory for a single chunk
    max_chunk_bytes = (available_memory_mb * 1024 * 1024) // 2

    # Calculate chunk size
    chunk_size = max_chunk_bytes // bytes_per_row

    # Ensure chunk size is between 1000 and 50000
    chunk_size = max(1000, min(50000, chunk_size))

    # If file is small, use smaller chunks
    estimated_total_rows = file_size_bytes // bytes_per_row
    if estimated_total_rows < chunk_size:
        chunk_size = max(1000, estimated_total_rows // 4)

    logging.info(f"Estimated chunk size: {chunk_size} rows (file size: {file_size_bytes} bytes)")
    return chunk_size

