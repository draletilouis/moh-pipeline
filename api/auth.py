"""
API Authentication Module
Provides API key-based authentication for the Uganda Health API
"""
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
import os
from typing import Dict

# API Key header configuration
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)


def get_api_key() -> str:
    """
    Get API key from environment variables

    Returns:
        str: The API key

    Raises:
        ValueError: If API_KEY is not set in environment
    """
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError(
            "API_KEY not set in environment. "
            "Please set API_KEY in conf/.env file"
        )
    return api_key


async def verify_api_key(api_key: str = Security(api_key_header)) -> Dict[str, any]:
    """
    Verify API key from request header

    This function validates the API key provided in the X-API-Key header
    against the key stored in environment variables.

    Args:
        api_key: The API key from the request header (injected by FastAPI)

    Returns:
        dict: Client information for future expansion
            {
                "client_name": str,
                "scopes": list,
                "rate_limit": int
            }

    Raises:
        HTTPException: 401 if API key is invalid or missing

    Example:
        @app.get("/protected-endpoint")
        async def protected(client: dict = Depends(verify_api_key)):
            return {"message": f"Hello {client['client_name']}"}
    """
    try:
        correct_api_key = get_api_key()
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

    if api_key != correct_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Return client info structure (ready for database-backed expansion)
    return {
        "client_name": "default",
        "scopes": ["read", "write"],
        "rate_limit": 1000  # requests per hour
    }


async def verify_api_key_optional(api_key: str = Security(api_key_header)) -> Dict[str, any]:
    """
    Optional API key verification

    Use this for endpoints that can work with or without authentication,
    but may return different data based on auth status.

    Returns None if no API key provided, otherwise validates like verify_api_key

    Args:
        api_key: The API key from the request header (optional)

    Returns:
        dict or None: Client information if authenticated, None otherwise
    """
    if not api_key:
        return None

    return await verify_api_key(api_key)
