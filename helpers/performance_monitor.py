"""
Performance and memory monitoring utilities.
"""

import logging
import os
import time
from contextlib import contextmanager

try:
    import psutil
except ImportError:
    psutil = None


class PerformanceMonitor:
    """Monitor performance and memory usage during pipeline execution."""

    def __init__(self, sync_id: str):
        self.sync_id = sync_id
        self.start_time = None
        self.checkpoints = {}
        self.memory_samples = []
        self.enabled = os.getenv('ENABLE_MEMORY_MONITORING', 'false').lower() == 'true'

        try:
            import psutil
            self.process = psutil.Process()
            self.psutil_available = True
        except ImportError:
            self.psutil_available = False
            if self.enabled:
                logging.warning("psutil not available, memory monitoring will be limited")

    def start(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self._log_memory("start")

    def checkpoint(self, name: str):
        """Record a checkpoint."""
        if self.start_time is None:
            self.start()

        elapsed = time.time() - self.start_time
        self.checkpoints[name] = elapsed

        self._log_memory(name)

        if self.enabled:
            logging.info(f"Checkpoint '{name}': {elapsed:.2f}s elapsed")

    def _log_memory(self, checkpoint_name: str):
        """Log current memory usage."""
        if not self.enabled:
            return

        try:
            if self.psutil_available:
                mem_info = self.process.memory_info()
                mem_mb = mem_info.rss / 1024 / 1024

                self.memory_samples.append({
                    'checkpoint': checkpoint_name,
                    'memory_mb': mem_mb,
                    'timestamp': time.time()
                })

                logging.info(
                    f"Memory at '{checkpoint_name}': {mem_mb:.2f} MB "
                    f"(VMS: {mem_info.vms / 1024 / 1024:.2f} MB)"
                )
            else:
                # Fallback to tracemalloc if psutil not available
                try:
                    import tracemalloc
                    if not tracemalloc.is_tracing():
                        tracemalloc.start()

                    current, peak = tracemalloc.get_traced_memory()
                    current_mb = current / 1024 / 1024
                    peak_mb = peak / 1024 / 1024

                    self.memory_samples.append({
                        'checkpoint': checkpoint_name,
                        'memory_mb': current_mb,
                        'peak_mb': peak_mb,
                        'timestamp': time.time()
                    })

                    logging.info(
                        f"Memory at '{checkpoint_name}': {current_mb:.2f} MB "
                        f"(peak: {peak_mb:.2f} MB)"
                    )
                except Exception:
                    pass
        except Exception as e:
            logging.debug(f"Failed to log memory: {e}")

    def get_summary(self) -> dict:
        """Get performance summary."""
        total_time = time.time() - self.start_time if self.start_time else 0

        summary = {
            'sync_id': self.sync_id,
            'total_time_seconds': total_time,
            'checkpoints': self.checkpoints,
        }

        if self.memory_samples:
            peak_memory = max(s['memory_mb'] for s in self.memory_samples)
            avg_memory = sum(s['memory_mb'] for s in self.memory_samples) / len(self.memory_samples)

            summary['peak_memory_mb'] = peak_memory
            summary['avg_memory_mb'] = avg_memory
            summary['memory_samples'] = self.memory_samples

        return summary

    def log_summary(self):
        """Log performance summary."""
        summary = self.get_summary()

        logging.info(
            f"Performance summary for {self.sync_id}: "
            f"total_time={summary['total_time_seconds']:.2f}s"
        )

        if 'peak_memory_mb' in summary:
            logging.info(
                f"Memory usage: peak={summary['peak_memory_mb']:.2f} MB, "
                f"avg={summary['avg_memory_mb']:.2f} MB"
            )

        for name, elapsed in summary['checkpoints'].items():
            logging.debug(f"  {name}: {elapsed:.2f}s")


@contextmanager
def monitor_performance(sync_id: str, enabled: bool = None):
    """
    Context manager for performance monitoring.

    Usage:
        with monitor_performance(sync_id) as monitor:
            # ... do work ...
            monitor.checkpoint('parsing')
            # ... more work ...
            monitor.checkpoint('validation')
    """
    if enabled is None:
        enabled = os.getenv('ENABLE_PERFORMANCE_MONITORING', 'true').lower() == 'true'

    monitor = PerformanceMonitor(sync_id)

    if enabled:
        monitor.start()

    try:
        yield monitor
    finally:
        if enabled:
            monitor.log_summary()


@contextmanager
def monitor_chunk_processing(chunk_num: int, chunk_size: int):
    """
    Context manager for monitoring individual chunk processing.

    Usage:
        with monitor_chunk_processing(chunk_num, chunk_size):
            # ... process chunk ...
    """
    enabled = os.getenv('ENABLE_CHUNK_MONITORING', 'false').lower() == 'true'

    if not enabled:
        yield
        return

    start_time = time.time()

    try:
        yield
    finally:
        elapsed = time.time() - start_time
        rows_per_sec = chunk_size / elapsed if elapsed > 0 else 0

        logging.info(
            f"Chunk {chunk_num}: processed {chunk_size} rows in {elapsed:.2f}s "
            f"({rows_per_sec:.0f} rows/sec)"
        )


def log_memory_warning_if_high(threshold_mb: float = 400):
    """
    Log a warning if memory usage is high.

    Args:
        threshold_mb: Memory threshold in MB
    """
    try:
        import psutil
        process = psutil.Process()
        mem_mb = process.memory_info().rss / 1024 / 1024

        if mem_mb > threshold_mb:
            logging.warning(
                f"High memory usage detected: {mem_mb:.2f} MB "
                f"(threshold: {threshold_mb} MB)"
            )
            return True
        return False
    except (ImportError, Exception):
        return False


def estimate_processing_rate(processed_rows: int, elapsed_seconds: float) -> dict:
    """
    Calculate processing rate statistics.

    Args:
        processed_rows: Number of rows processed
        elapsed_seconds: Time elapsed in seconds

    Returns:
        dict with rate statistics
    """
    if elapsed_seconds <= 0:
        return {
            'rows_per_second': 0,
            'seconds_per_1000_rows': 0,
            'estimated_hours_for_1m_rows': 0
        }

    rows_per_sec = processed_rows / elapsed_seconds
    secs_per_1000 = 1000 / rows_per_sec if rows_per_sec > 0 else 0
    hours_per_1m = (1_000_000 / rows_per_sec / 3600) if rows_per_sec > 0 else 0

    return {
        'rows_per_second': rows_per_sec,
        'seconds_per_1000_rows': secs_per_1000,
        'estimated_hours_for_1m_rows': hours_per_1m
    }

