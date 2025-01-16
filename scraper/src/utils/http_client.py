"""
HTTP client utilities for making resilient async requests with retry capabilities.
Created by: gabes-machado
Date: 2025-01-16
"""

import asyncio
import logging
from typing import Optional, Union, Dict, Any
from datetime import datetime

import aiohttp
from aiohttp import ClientTimeout
from asyncio import TimeoutError
from aiohttp.client_exceptions import ClientError
import backoff

# Configure logging
logger = logging.getLogger(__name__)

class HTTPClientError(Exception):
    """Custom exception for HTTP client errors"""
    pass

class AsyncHTTPClient:
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        status_forcelist: tuple = (429, 500, 502, 503, 504),
    ):
        self.timeout = ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        await self.create_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_session()

    async def create_session(self) -> None:
        """Create aiohttp session with custom headers and configuration"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; CustomScraper/1.0; +http://example.com)',
                }
            )

    async def close_session(self) -> None:
        """Close the aiohttp session"""
        if self._session and not self._session.closed:
            await self._session.close()

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if the request should be retried based on the exception"""
        if isinstance(exception, TimeoutError):
            return True
        if isinstance(exception, aiohttp.ClientResponseError):
            return exception.status in self.status_forcelist
        return isinstance(exception, (ClientError, ConnectionError))

    @backoff.on_exception(
        backoff.expo,
        (TimeoutError, ClientError, ConnectionError),
        max_tries=5,
        logger=logger
    )
    async def get(
        self, 
        url: str, 
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """
        Perform an async GET request with automatic retries and exponential backoff
        
        Args:
            url: The URL to request
            params: Optional query parameters
            **kwargs: Additional arguments to pass to aiohttp.ClientSession.get()
            
        Returns:
            str: The response text
            
        Raises:
            HTTPClientError: If the request fails after all retries
        """
        if not self._session or self._session.closed:
            await self.create_session()

        try:
            async with self._session.get(url, params=params, **kwargs) as response:
                response.raise_for_status()
                return await response.text()

        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            raise HTTPClientError(f"Failed to fetch {url}: {str(e)}") from e