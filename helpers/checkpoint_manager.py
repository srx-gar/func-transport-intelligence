"""
Checkpoint Manager for Parquet-Based Fault-Tolerant Processing

This module provides checkpoint functionality for large file ETL processing,
enabling resumable operations by saving intermediate results as parquet files.
"""

import os
import json
import logging
import shutil
from datetime import datetime, timezone
from typing import List, Dict, Optional
import pandas as pd


class CheckpointManager:
    """
    Manages parquet-based checkpoints for resumable large file processing.

    Architecture:
    - Phase 1: Convert .dat chunks to .parquet files (one-time)
    - Phase 2: Process .parquet chunks (resumable from any point)

    Directory structure:
        checkpoints/
          {sync_id}/
            manifest.json           # Metadata and status tracking
            chunk_0000.parquet      # Chunk 0 data
            chunk_0001.parquet      # Chunk 1 data
            ...
    """

    def __init__(self, sync_id: str, source_file: str, checkpoint_base_path: str):
        """
        Initialize checkpoint manager.

        Args:
            sync_id: Unique sync identifier
            source_file: Name of source file being processed
            checkpoint_base_path: Base directory for checkpoints (e.g., 'checkpoints')
        """
        self.sync_id = sync_id
        self.source_file = source_file
        self.checkpoint_base_path = checkpoint_base_path
        self.checkpoint_dir = os.path.join(checkpoint_base_path, sync_id)
        self.manifest_path = os.path.join(self.checkpoint_dir, 'manifest.json')
        self.manifest: Optional[Dict] = None

    def checkpoint_exists(self) -> bool:
        """Check if a checkpoint exists for this sync."""
        exists = os.path.exists(self.manifest_path)
        if exists:
            try:
                self._load_manifest()
                logging.info(f"♻️  Found existing checkpoint for sync {self.sync_id}")
                return True
            except Exception as e:
                logging.warning(f"Checkpoint manifest exists but is corrupted: {e}")
                return False
        return False

    def create_checkpoint(
        self,
        total_chunks: int,
        chunk_size: int,
        file_size_bytes: int,
        source_path: str = ""
    ):
        """
        Initialize a new checkpoint manifest.

        Args:
            total_chunks: Total number of chunks to process
            chunk_size: Number of rows per chunk
            file_size_bytes: Size of source file in bytes
            source_path: Full path to source file
        """
        os.makedirs(self.checkpoint_dir, exist_ok=True)

        self.manifest = {
            'sync_id': self.sync_id,
            'source_file': self.source_file,
            'source_path': source_path,
            'file_size_bytes': file_size_bytes,
            'total_chunks': total_chunks,
            'chunk_size': chunk_size,
            'status': 'converting',  # converting -> processing -> completed
            'phase': 'conversion',   # conversion or processing
            'created_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'chunks': []
        }

        self._save_manifest()
        logging.info(
            f"📦 Created checkpoint for {total_chunks} chunks "
            f"(chunk_size={chunk_size}, file_size={file_size_bytes:,} bytes)"
        )

    def save_chunk_parquet(self, chunk_id: int, chunk_df: pd.DataFrame) -> Dict:
        """
        Save a chunk as parquet file.

        Args:
            chunk_id: Sequential chunk identifier (0-based)
            chunk_df: DataFrame containing chunk data

        Returns:
            Chunk metadata dictionary
        """
        parquet_filename = f"chunk_{chunk_id:04d}.parquet"
        parquet_path = os.path.join(self.checkpoint_dir, parquet_filename)

        # Save with optimal compression (snappy is fast, good compression)
        chunk_df.to_parquet(
            parquet_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )

        # Get file size
        parquet_size = os.path.getsize(parquet_path)

        # Create chunk metadata
        chunk_info = {
            'chunk_id': chunk_id,
            'status': 'ready',  # ready to be processed
            'rows': len(chunk_df),
            'parquet_path': parquet_filename,
            'parquet_size_bytes': parquet_size,
            'created_at': datetime.now(timezone.utc).isoformat()
        }

        # Add to manifest
        if self.manifest is None:
            self._load_manifest()

        self.manifest['chunks'].append(chunk_info)
        self.manifest['updated_at'] = datetime.now(timezone.utc).isoformat()
        self._save_manifest()

        # Calculate compression ratio
        original_estimate = len(chunk_df) * 1024  # Rough estimate
        compression_ratio = parquet_size / original_estimate if original_estimate > 0 else 0

        logging.info(
            f"✅ Saved chunk {chunk_id}: {len(chunk_df):,} rows → "
            f"{parquet_size:,} bytes ({compression_ratio:.1%} of estimated original)"
        )

        return chunk_info

    def finalize_conversion(self):
        """Mark conversion phase as complete."""
        if self.manifest is None:
            self._load_manifest()

        self.manifest['status'] = 'ready'
        self.manifest['phase'] = 'processing'
        self.manifest['conversion_completed_at'] = datetime.now(timezone.utc).isoformat()
        self.manifest['updated_at'] = datetime.now(timezone.utc).isoformat()
        self._save_manifest()

        total_size = sum(c.get('parquet_size_bytes', 0) for c in self.manifest['chunks'])
        logging.info(
            f"✅ Conversion complete: {len(self.manifest['chunks'])} chunks, "
            f"{total_size / 1024 / 1024:.2f} MB total"
        )

    def get_pending_chunks(self) -> List[int]:
        """
        Get list of chunk IDs that need processing.

        Returns:
            List of chunk IDs with status in ['ready', 'failed', 'pending']
        """
        if self.manifest is None:
            self._load_manifest()

        pending_chunks = [
            chunk['chunk_id']
            for chunk in self.manifest['chunks']
            if chunk['status'] in ['ready', 'failed', 'pending']
        ]

        return sorted(pending_chunks)

    def get_total_chunks(self) -> int:
        """Get total number of chunks."""
        if self.manifest is None:
            self._load_manifest()
        return self.manifest.get('total_chunks', 0)

    def load_chunk(self, chunk_id: int) -> pd.DataFrame:
        """
        Load a chunk from parquet file.

        Args:
            chunk_id: Chunk identifier to load

        Returns:
            DataFrame containing chunk data
        """
        parquet_filename = f"chunk_{chunk_id:04d}.parquet"
        parquet_path = os.path.join(self.checkpoint_dir, parquet_filename)

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Chunk parquet file not found: {parquet_path}")

        # Load parquet (60x faster than parsing .dat)
        chunk_df = pd.read_parquet(parquet_path, engine='pyarrow')

        logging.debug(f"📦 Loaded chunk {chunk_id}: {len(chunk_df):,} rows from parquet")

        return chunk_df

    def mark_chunk_complete(
        self,
        chunk_id: int,
        records_inserted: int,
        records_updated: int,
        validation_errors: int = 0
    ):
        """
        Mark a chunk as successfully processed.

        Args:
            chunk_id: Chunk identifier
            records_inserted: Number of records inserted to database
            records_updated: Number of records updated in database
            validation_errors: Number of validation errors encountered
        """
        if self.manifest is None:
            self._load_manifest()

        for chunk in self.manifest['chunks']:
            if chunk['chunk_id'] == chunk_id:
                chunk['status'] = 'completed'
                chunk['processed_at'] = datetime.now(timezone.utc).isoformat()
                chunk['records_inserted'] = records_inserted
                chunk['records_updated'] = records_updated
                chunk['validation_errors'] = validation_errors
                break

        self.manifest['updated_at'] = datetime.now(timezone.utc).isoformat()
        self._save_manifest()

        logging.info(
            f"✅ Chunk {chunk_id} marked complete: "
            f"+{records_inserted} inserted, +{records_updated} updated"
        )

    def mark_chunk_failed(self, chunk_id: int, error_message: str):
        """
        Mark a chunk as failed with error details.

        Args:
            chunk_id: Chunk identifier
            error_message: Error description
        """
        if self.manifest is None:
            self._load_manifest()

        for chunk in self.manifest['chunks']:
            if chunk['chunk_id'] == chunk_id:
                chunk['status'] = 'failed'
                chunk['failed_at'] = datetime.now(timezone.utc).isoformat()
                chunk['error'] = error_message[:500]  # Truncate long errors
                break

        self.manifest['updated_at'] = datetime.now(timezone.utc).isoformat()
        self._save_manifest()

        logging.warning(f"❌ Chunk {chunk_id} marked failed: {error_message}")

    def get_progress_summary(self) -> Dict:
        """
        Get current processing progress statistics.

        Returns:
            Dictionary with progress metrics
        """
        if self.manifest is None:
            self._load_manifest()

        chunks = self.manifest.get('chunks', [])
        total = len(chunks)

        completed = sum(1 for c in chunks if c['status'] == 'completed')
        failed = sum(1 for c in chunks if c['status'] == 'failed')
        pending = sum(1 for c in chunks if c['status'] in ['ready', 'pending'])

        total_inserted = sum(c.get('records_inserted', 0) for c in chunks if c['status'] == 'completed')
        total_updated = sum(c.get('records_updated', 0) for c in chunks if c['status'] == 'completed')
        total_errors = sum(c.get('validation_errors', 0) for c in chunks)

        return {
            'total_chunks': total,
            'completed_chunks': completed,
            'failed_chunks': failed,
            'pending_chunks': pending,
            'completion_rate': completed / total if total > 0 else 0,
            'total_records_inserted': total_inserted,
            'total_records_updated': total_updated,
            'total_validation_errors': total_errors,
            'status': self.manifest.get('status', 'unknown'),
            'phase': self.manifest.get('phase', 'unknown')
        }

    def cleanup(self, force: bool = False):
        """
        Remove checkpoint directory and all parquet files.

        Args:
            force: If True, cleanup even if not all chunks are completed
        """
        if self.manifest is None:
            self._load_manifest()

        # Safety check: only cleanup if all chunks completed (unless forced)
        if not force:
            pending = self.get_pending_chunks()
            if pending:
                logging.warning(
                    f"Skipping cleanup: {len(pending)} chunks still pending. "
                    f"Use force=True to cleanup anyway."
                )
                return

        # Remove checkpoint directory
        if os.path.exists(self.checkpoint_dir):
            shutil.rmtree(self.checkpoint_dir)
            logging.info(f"🗑️  Cleaned up checkpoint directory: {self.checkpoint_dir}")
        else:
            logging.warning(f"Checkpoint directory not found: {self.checkpoint_dir}")

    def _load_manifest(self):
        """Load manifest from JSON file."""
        if not os.path.exists(self.manifest_path):
            raise FileNotFoundError(f"Manifest not found: {self.manifest_path}")

        with open(self.manifest_path, 'r') as f:
            self.manifest = json.load(f)

    def _save_manifest(self):
        """Save manifest to JSON file."""
        os.makedirs(self.checkpoint_dir, exist_ok=True)

        with open(self.manifest_path, 'w') as f:
            json.dump(self.manifest, f, indent=2)


def should_use_checkpointing(file_size_bytes: int, config_threshold_mb: int = 20) -> bool:
    """
    Determine if checkpointing should be used based on file size.

    Args:
        file_size_bytes: Size of file in bytes
        config_threshold_mb: Threshold in MB from config

    Returns:
        True if file is large enough to benefit from checkpointing
    """
    file_size_mb = file_size_bytes / 1024 / 1024
    return file_size_mb >= config_threshold_mb

