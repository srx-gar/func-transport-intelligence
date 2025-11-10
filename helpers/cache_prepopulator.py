"""Helper module for triggering cache prepopulation in the transport intelligence service."""

import logging
import os
from typing import Optional, Dict, Any
import urllib.request
import urllib.error
import json


def trigger_cache_prepopulation(
    service_url: Optional[str] = None,
    clear_first: bool = True,
    concurrency: int = 15,
    timeout_seconds: int = 30
) -> Dict[str, Any]:
    """
    Trigger async cache prepopulation in the transport intelligence service.

    This function calls the /api/v1/cache/prepopulate endpoint to initiate
    an asynchronous cache prepopulation job. The job runs in the background
    and this function returns immediately with job information.

    Args:
        service_url: Base URL of the transport intelligence service
                    (defaults to TRANSPORT_INTELLIGENCE_SERVICE_URL env var)
        clear_first: Whether to clear existing cache before prepopulating
        concurrency: Number of concurrent requests for prepopulation (1-20)
        timeout_seconds: HTTP request timeout in seconds

    Returns:
        Dict containing job information:
        {
            'success': bool,
            'jobId': str,           # Date-based job ID (YYYYMMDD)
            'status': str,          # 'pending' or 'in_progress'
            'alreadyRunning': bool, # True if job was already running
            'statusUrl': str        # URL to check job status
        }

    Raises:
        Exception: If the HTTP request fails or returns an error status
    """
    logger = logging.getLogger(__name__)

    # Get service URL from parameter or environment
    if not service_url:
        service_url = os.getenv('TRANSPORT_INTELLIGENCE_SERVICE_URL')

    if not service_url:
        raise ValueError(
            "TRANSPORT_INTELLIGENCE_SERVICE_URL environment variable not set "
            "and service_url parameter not provided"
        )

    # Ensure URL doesn't end with slash
    service_url = service_url.rstrip('/')

    # Build endpoint URL with query parameters
    endpoint = f"{service_url}/api/v1/cache/prepopulate"
    params = f"?clearFirst={'true' if clear_first else 'false'}&concurrency={concurrency}"
    full_url = f"{endpoint}{params}"

    logger.info(f"Triggering cache prepopulation: {full_url}")
    logger.info(f"Options: clearFirst={clear_first}, concurrency={concurrency}")

    try:
        # Create POST request
        req = urllib.request.Request(
            full_url,
            method='POST',
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Azure-Function-ETL/1.0'
            }
        )

        # Make request with timeout
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            response_data = response.read()
            result = json.loads(response_data.decode('utf-8'))

            # Log result
            if result.get('success'):
                if result.get('alreadyRunning'):
                    logger.info(
                        f"Cache prepopulation job {result.get('jobId')} is already "
                        f"{result.get('status')} - not starting a new job"
                    )
                else:
                    logger.info(
                        f"Cache prepopulation job {result.get('jobId')} started successfully. "
                        f"Status URL: {result.get('statusUrl')}"
                    )
            else:
                logger.warning(f"Cache prepopulation returned success=false: {result}")

            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else 'No error body'
        logger.error(
            f"HTTP error triggering cache prepopulation: {e.code} {e.reason}\n"
            f"Response: {error_body}"
        )
        raise Exception(
            f"Failed to trigger cache prepopulation: HTTP {e.code} {e.reason}"
        ) from e

    except urllib.error.URLError as e:
        logger.error(f"URL error triggering cache prepopulation: {e.reason}")
        raise Exception(
            f"Failed to connect to transport intelligence service: {e.reason}"
        ) from e

    except Exception as e:
        logger.error(f"Unexpected error triggering cache prepopulation: {str(e)}")
        raise


def trigger_cache_prepopulation_safe(
    service_url: Optional[str] = None,
    clear_first: bool = True,
    concurrency: int = 15
) -> bool:
    """
    Safe wrapper for trigger_cache_prepopulation that catches and logs exceptions.

    This version doesn't raise exceptions - it returns True/False and logs errors.
    Use this when cache prepopulation is optional and shouldn't block the ETL process.

    Args:
        service_url: Base URL of the transport intelligence service
        clear_first: Whether to clear existing cache before prepopulating
        concurrency: Number of concurrent requests for prepopulation (1-20)

    Returns:
        True if trigger was successful, False if it failed
    """
    logger = logging.getLogger(__name__)

    try:
        result = trigger_cache_prepopulation(
            service_url=service_url,
            clear_first=clear_first,
            concurrency=concurrency
        )
        return result.get('success', False)
    except Exception as e:
        logger.warning(
            f"Failed to trigger cache prepopulation (non-critical): {str(e)}\n"
            "Continuing with ETL process..."
        )
        return False
