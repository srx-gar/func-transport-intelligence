"""Small helper to load a local .env for development (Docker/local DB).

This is intentionally minimal: it only loads a .env file from the repo root
(if present) and does not override existing environment variables unless
explicitly allowed.

Usage:
    from helpers.env_loader import load_local_env
    load_local_env()

"""
from pathlib import Path
import logging
import os
from dotenv import load_dotenv


def load_local_env(dotenv_filename: str = '.env', override: bool = False) -> bool:
    """Load environment variables from a .env file in the repo root.

    Returns True if a .env file was found and loaded, False otherwise.
    """
    repo_root = Path(__file__).resolve().parents[1]
    dotenv_path = repo_root / dotenv_filename

    if not dotenv_path.exists():
        logging.debug("No local .env found at %s", dotenv_path)
        return False

    # load_dotenv returns True even if some variables already set; we respect `override`
    load_dotenv(dotenv_path=str(dotenv_path), override=override)
    logging.info("Loaded local .env from %s (override=%s)", dotenv_path, override)
    return True

