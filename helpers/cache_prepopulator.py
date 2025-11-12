"""Helper module for triggering cache repopulation in the transport intelligence service."""

import logging
import os
from typing import Optional, Dict, Any
import urllib.request
import urllib.error
import json
from urllib.parse import urlencode


def trigger_cache_repopulation(
    service_url: Optional[str] = None,
    concurrency: int = 10,
    timeout_seconds: int = 30,
    clear_first: bool = False,
) -> Dict[str, Any]:
    """
    Trigger async cache repopulation in the transport intelligence service.

    This function calls the /api/v1/cache/repopulate endpoint to initiate
    an asynchronous cache repopulation job. The job runs in the background
    and this function returns immediately with job information.

    Args:
        service_url: Base URL of the transport intelligence service
                    (defaults to TRANSPORT_INTELLIGENCE_SERVICE_URL env var)
        concurrency: Number of concurrent requests for repopulation (1-20)
        timeout_seconds: HTTP request timeout in seconds
        clear_first: Whether to clear existing cache before repopulating

    Returns:
        Dict containing job information:
        {
            'success': bool,
            'jobId': str,           # Job ID
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
    endpoint = f"{service_url}/api/v1/cache/repopulate"
    # Include clearFirst flag as a query parameter if requested by caller.
    # Use camelCase 'clearFirst' to match common API conventions; server may ignore unknown params.
    params_dict = {'concurrency': concurrency, 'clearFirst': 'true' if clear_first else 'false'}
    params = urlencode(params_dict)
    full_url = f"{endpoint}?{params}"

    logger.info(f"Triggering cache repopulation: {full_url}")
    logger.info(f"Options: concurrency={concurrency}")

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

            # Log jobId and statusUrl for monitoring
            job_id = result.get('jobId')
            status_url = result.get('statusUrl')
            logger.info(f"Cache repopulation triggered. Job ID: {job_id}, Status URL: {status_url}")

            if result.get('success'):
                if result.get('alreadyRunning'):
                    logger.info(
                        f"Cache repopulation job {job_id} is already "
                        f"{result.get('status')} - not starting a new job"
                    )
                else:
                    logger.info(
                        f"Cache repopulation job {job_id} started successfully. "
                        f"Status URL: {status_url}"
                    )
            else:
                logger.warning(f"Cache repopulation returned success=false: {result}")

            return result

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else 'No error body'
        logger.error(
            f"HTTP error triggering cache repopulation: {e.code} {e.reason}\n"
            f"Response: {error_body}"
        )
        raise Exception(
            f"Failed to trigger cache repopulation: HTTP {e.code} {e.reason}"
        ) from e

    except urllib.error.URLError as e:
        logger.error(f"URL error triggering cache repopulation: {e.reason}")
        raise Exception(
            f"Failed to connect to transport intelligence service: {e.reason}"
        ) from e

    except Exception as e:
        logger.error(f"Unexpected error triggering cache repopulation: {str(e)}")
        raise


def trigger_cache_repopulation_safe(
    service_url: Optional[str] = None,
    concurrency: int = 10,
    clear_first: bool = False,
) -> bool:
    """
    Safe wrapper for trigger_cache_repopulation that catches and logs exceptions.

    This version doesn't raise exceptions - it returns True/False and logs errors.
    Use this when cache repopulation is optional and shouldn't block the ETL process.

    Args:
        service_url: Base URL of the transport intelligence service
        concurrency: Number of concurrent requests for repopulation (1-20)
        clear_first: Whether to clear existing cache before repopulating

    Returns:
        True if trigger was successful, False if it failed
    """
    logger = logging.getLogger(__name__)

    try:
        # Pass-through clear_first support for backward-compatible callers
        result = trigger_cache_repopulation(
            service_url=service_url,
            concurrency=concurrency,
            clear_first=clear_first
        )
        return result.get('success', False)
    except Exception as e:
        logger.warning(
            f"Failed to trigger cache repopulation (non-critical): {str(e)}\n"
            "Continuing with ETL process..."
        )
        return False
