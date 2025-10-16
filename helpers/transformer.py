"""
Data transformation utilities for ZTDWR to transport_documents schema
"""

import pandas as pd
from datetime import datetime
import logging


def transform_to_transport_documents(
    df: pd.DataFrame,
    sync_id: str,
    file_name: str
) -> pd.DataFrame:
    """
    Transform ZTDWR DataFrame to transport_documents schema.

    Maps SAP column names to PostgreSQL column names and applies
    data type conversions.
    """

    # Column mapping (SAP -> PostgreSQL)
    # This is a basic mapping - adjust based on actual SAP column names
    column_mapping = {
        'SPB_ID': 'spb_id',
        'WAKTU_TIMBANG_TERIMA': 'waktu_timbang_terima',
        'TGL_SPB': 'tgl_spb',
        'DRIVER_NIK': 'driver_nik',
        'DRIVER_NAME': 'driver_name',
        'NOPOL': 'nopol',
        'JENIS_KENDARAAN': 'jenis_kendaraan',
        'JENIS_ANGKUTAN': 'jenis_angkutan',
        'ORIGIN': 'origin',
        'DESTINATION': 'destination',
        'BRUTO_TIMBANG': 'bruto_timbang',
        'NETTO_TIMBANG': 'netto_timbang',
        'BBM_ACTUAL': 'bbm_actual',
        'KILOMETER_ACTUAL': 'kilometer_actual',
        'TERRITORY_ID': 'territory_id',
    }

    # Create copy and rename only columns that exist
    transformed = df.copy()
    existing_mappings = {k: v for k, v in column_mapping.items() if k in df.columns}
    transformed = transformed.rename(columns=existing_mappings)

    # Data type conversions
    if 'waktu_timbang_terima' in transformed.columns:
        transformed['waktu_timbang_terima'] = pd.to_datetime(
            transformed['waktu_timbang_terima'],
            errors='coerce'
        )

    if 'tgl_spb' in transformed.columns:
        transformed['tgl_spb'] = pd.to_datetime(
            transformed['tgl_spb'],
            errors='coerce'
        ).dt.date

    # Numeric conversions
    numeric_cols = [
        'bruto_timbang', 'netto_timbang',
        'bbm_actual', 'kilometer_actual'
    ]
    for col in numeric_cols:
        if col in transformed.columns:
            transformed[col] = pd.to_numeric(
                transformed[col],
                errors='coerce'
            )

    # Add audit columns
    transformed['created_at'] = datetime.utcnow()
    transformed['updated_at'] = datetime.utcnow()
    transformed['sync_id'] = sync_id
    transformed['source_file'] = file_name

    logging.info(f"Transformed {len(transformed)} records to transport_documents schema")

    return transformed
