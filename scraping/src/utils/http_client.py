"""
HTTP client utilities for making resilient async requests.
Author: gabes-machado
Created: 2025-01-17 01:44:34 UTC
"""

import asyncio
import logging
from typing import Optional, Dict, Any
import datetime
import time

import aiohttp
from aiohttp import ClientTimeout, ClientResponse
from asyncio import TimeoutError
from aiohttp.client_exceptions import (
    ClientError, 
    ClientResponseError, 
    ServerDisconnectedError,
    TooManyRedirects
)
import backoff
from yarl import URL

logger = logging.getLogger(__name__)

class HTTPClientError(Exception):
    """Custom exception for HTTP client errors with detailed information"""
    def __init__(self, message: str, url: str, status: Optional[int] = None):
        self.url = url
        self.status = status
        super().__init__(f"{message} (URL: {url}, Status: {status})")

class AsyncHTTPClient:
    """Asynchronous HTTP client with retry logic and encoding handling"""
    
    # Common Brazilian Portuguese encodings
    ENCODINGS = ['iso-8859-1', 'utf-8', 'cp1252', 'latin1']
    
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        status_forcelist: tuple = (429, 500, 502, 503, 504),
        max_concurrent_requests: int = 10
    ):
        """
        Initialize the HTTP client with configuration
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            backoff_factor: Exponential backoff multiplier
            status_forcelist: HTTP status codes to retry on
            max_concurrent_requests: Maximum concurrent requests
        """
        self.timeout = ClientTimeout(
            total=timeout,
            connect=timeout/2,
            sock_read=timeout
        )
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._last_request_time = 0
        self._min_request_interval = 0.1  # 100ms between requests
        
        logger.info(
            f"Initialized AsyncHTTPClient (timeout={timeout}s, "
            f"max_retries={max_retries}, max_concurrent={max_concurrent_requests})"
        )

    async def __aenter__(self):
        """Async context manager entry"""
        await self.create_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup"""
        await self.close_session()
        if exc_type:
            logger.error(f"Error in context: {exc_type.__name__}: {exc_val}")

    async def create_session(self) -> None:
        """Create aiohttp session with custom configuration"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers=self._get_default_headers(),
                raise_for_status=True,
                trust_env=True
            )
            logger.debug("Created new aiohttp session")

    async def close_session(self) -> None:
        """Safely close the aiohttp session"""
        if self._session and not self._session.closed:
            try:
                await self._session.close()
                logger.debug("Closed aiohttp session")
                
                # Wait for all connections to close
                await asyncio.sleep(0.25)
            except Exception as e:
                logger.warning(f"Error closing session: {e}")

    def _get_default_headers(self) -> Dict[str, str]:
        """Get default headers for requests"""
        return {
            'User-Agent': (
                'Mozilla/5.0 (compatible; CustomScraper/1.0; '
                '+https://github.com/gabes-machado)'
            ),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Connection': 'keep-alive'
        }

    def _should_retry(self, exception: Exception) -> bool:
        """
        Determine if the request should be retried based on the exception
        
        Args:
            exception: The exception that occurred
            
        Returns:
            bool: True if should retry, False otherwise
        """
        if isinstance(exception, TimeoutError):
            return True
        if isinstance(exception, ClientResponseError):
            return exception.status in self.status_forcelist
        return isinstance(exception, (
            ClientError,
            ConnectionError,
            ServerDisconnectedError
        ))

    async def _decode_response(
        self, 
        response: ClientResponse, 
        content: bytes
    ) -> str:
        """
        Attempt to decode response content with multiple encodings
        
        Args:
            response: The aiohttp response
            content: The raw content bytes
            
        Returns:
            str: Decoded content
            
        Raises:
            UnicodeError: If content cannot be decoded
        """
        # Try content-type encoding first
        if response.charset:
            try:
                return content.decode(response.charset)
            except UnicodeError:
                logger.debug(f"Failed to decode with charset {response.charset}")

        # Try common encodings
        errors = []
        for encoding in self.ENCODINGS:
            try:
                return content.decode(encoding)
            except UnicodeError as e:
                errors.append(f"{encoding}: {str(e)}")

        # If all fails, try with 'ignore' error handler
        logger.warning(
            f"Failed to decode with encodings: {', '.join(errors)}. "
            "Falling back to utf-8 with ignore."
        )
        return content.decode('utf-8', errors='ignore')

    async def _throttle_request(self) -> None:
        """Implement request throttling"""
        now = time.time()
        time_since_last = now - self._last_request_time
        if time_since_last < self._min_request_interval:
            await asyncio.sleep(self._min_request_interval - time_since_last)
        self._last_request_time = time.time()

    @backoff.on_exception(
        backoff.expo,
        (TimeoutError, ClientError, ConnectionError, ServerDisconnectedError),
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
        Perform an async GET request with automatic retries and throttling
        
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

        # Validate URL
        try:
            parsed_url = URL(url)
            if not parsed_url.scheme or not parsed_url.host:
                raise ValueError("Invalid URL")
        except Exception as e:
            raise HTTPClientError(f"Invalid URL: {str(e)}", url)

        async with self._semaphore:
            await self._throttle_request()
            
            try:
                start_time = time.time()
                async with self._session.get(
                    url, 
                    params=params,
                    allow_redirects=True,
                    max_redirects=5,
                    **kwargs
                ) as response:
                    content = await response.read()
                    
                    # Log request details
                    duration = time.time() - start_time
                    logger.debug(
                        f"GET {url} - Status: {response.status} - "
                        f"Duration: {duration:.2f}s - "
                        f"Size: {len(content)} bytes"
                    )

                    return await self._decode_response(response, content)

            except TooManyRedirects as e:
                logger.error(f"Too many redirects for {url}")
                raise HTTPClientError("Too many redirects", url) from e
                
            except asyncio.TimeoutError as e:
                logger.error(f"Timeout fetching {url}")
                raise HTTPClientError("Request timeout", url) from e
                
            except ClientResponseError as e:
                logger.error(f"HTTP {e.status} error for {url}: {str(e)}")
                raise HTTPClientError(str(e), url, e.status) from e
                
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                raise HTTPClientError(str(e), url) from e