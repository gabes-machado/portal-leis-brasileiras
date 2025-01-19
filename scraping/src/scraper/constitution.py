"""
Main constitution scraper implementation.
Author: gabes-machado
Created: 2025-01-17 01:50:49 UTC
Updated: 2025-01-19 20:25:35 UTC
"""

import logging
import asyncio
import json
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

from utils.http_client import AsyncHTTPClient, HTTPClientError
from utils.html_parser import HTMLParser
from utils.constitution_structure import ConstitutionProcessor
from utils.json_handler import JSONHandler
from utils.schema import ConstitutionSchema

logger = logging.getLogger(__name__)

class ConstitutionScraperError(Exception):
    """Custom exception for constitution scraping errors"""
    pass

class ConstitutionScraper:
    """Main scraper class for Brazilian Constitution"""

    def __init__(
        self, 
        base_url: str = "https://www.planalto.gov.br",
        max_retries: int = 3,
        timeout: int = 30
    ):
        """
        Initialize the constitution scraper
        
        Args:
            base_url: Base URL for the Planalto website
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.constitution_path = "/ccivil_03/constituicao/constituicao.htm"
        self.max_retries = max_retries
        self.timeout = timeout
        self.stats = self._init_stats()
        self.schema_validator = ConstitutionSchema()
        
        logger.info(
            f"Initialized ConstitutionScraper "
            f"(base_url={base_url}, max_retries={max_retries})"
        )

    def _init_stats(self) -> Dict[str, Any]:
        """Initialize statistics tracking"""
        return {
            "start_time": None,
            "end_time": None,
            "total_elements": 0,
            "processed_elements": 0,
            "errors": 0,
            "warnings": 0,
            "element_counts": {
                "preambulo": 0,
                "titulos": 0,
                "capitulos": 0,
                "secoes": 0,
                "subsecoes": 0,
                "artigos": 0,
                "paragrafos": 0,
                "incisos": 0,
                "alineas": 0,
                "adct": 0
            }
        }

    def _update_stats(self, **kwargs) -> None:
        """Update scraping statistics"""
        self.stats.update(kwargs)

    def _update_element_count(self, element_type: str) -> None:
        """Update element type counter"""
        element_type = element_type.lower()
        if element_type in self.stats["element_counts"]:
            self.stats["element_counts"][element_type] += 1

    def _log_stats(self) -> None:
        """Log scraping statistics"""
        if self.stats["start_time"] and self.stats["end_time"]:
            duration = self.stats["end_time"] - self.stats["start_time"]
            logger.info(
                f"Scraping completed in {duration.total_seconds():.2f} seconds"
            )
            logger.info(
                f"Processed {self.stats['processed_elements']} of "
                f"{self.stats['total_elements']} elements"
            )
            
            # Log element type statistics
            logger.info("Element counts:")
            for element_type, count in self.stats["element_counts"].items():
                if count > 0:
                    logger.info(f"  {element_type}: {count}")
                    
            logger.info(f"Errors: {self.stats['errors']}")
            logger.info(f"Warnings: {self.stats['warnings']}")

    async def _fetch_html(self) -> str:
        """
        Fetch HTML content with retry logic
        
        Returns:
            str: HTML content
            
        Raises:
            ConstitutionScraperError: If fetching fails after retries
        """
        url = f"{self.base_url}{self.constitution_path}"
        
        for attempt in range(self.max_retries):
            try:
                async with AsyncHTTPClient(
                    timeout=self.timeout,
                    max_retries=2
                ) as client:
                    content = await client.get(url)
                    
                    if not content:
                        raise ConstitutionScraperError("Empty response received")
                        
                    logger.info(
                        f"Successfully fetched {len(content)} bytes "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    return content
                    
            except HTTPClientError as e:
                logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries} failed: {e}"
                )
                if attempt == self.max_retries - 1:
                    raise ConstitutionScraperError(
                        f"Failed to fetch constitution after {self.max_retries} attempts"
                    ) from e
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def _validate_output_path(self, output_file: str) -> None:
        """
        Validate output file path
        
        Args:
            output_file: Path to output file
            
        Raises:
            ConstitutionScraperError: If path is invalid
        """
        try:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if output_path.exists():
                logger.warning(f"Output file {output_file} will be overwritten")
                
        except Exception as e:
            raise ConstitutionScraperError(f"Invalid output path: {e}")

    async def scrape(self, output_file: str) -> bool:
        """
        Execute the complete scraping process
        
        Args:
            output_file: Path to save the JSON output
            
        Returns:
            bool: True if successful, False otherwise
        """
        self._update_stats(start_time=datetime.utcnow())
        
        try:
            # Validate output path
            self._validate_output_path(output_file)

            # Fetch HTML content
            html_content = await self._fetch_html()
            if not html_content:
                raise ConstitutionScraperError("Failed to fetch HTML content")

            # Initialize parser and processor
            parser = HTMLParser(html_content)
            parser.remove_strike_tags()
            processor = ConstitutionProcessor()
            
            # Process constitutional elements
            elements = list(parser.iter_constitutional_elements())
            self._update_stats(total_elements=len(elements))
            
            for idx, (element_type, number, title, text) in enumerate(elements, 1):
                try:
                    processor.process_element(element_type, number, title, text)
                    self._update_stats(processed_elements=idx)
                    self._update_element_count(element_type)
                    
                    if idx % 50 == 0:  # Progress update every 50 elements
                        logger.info(
                            f"Progress: {idx}/{len(elements)} "
                            f"({idx/len(elements)*100:.1f}%)"
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing element {idx}: {e}")
                    self._update_stats(errors=self.stats["errors"] + 1)

            # Get and validate result
            result = processor.get_result()
            if not result:
                raise ConstitutionScraperError("Empty processing result")
            
            # Validate against schema before saving
            if not self.schema_validator.validate_data(result):
                raise ConstitutionScraperError("Schema validation failed")
                
            # Save validated result
            JSONHandler.save_json(result, output_file)
            
            logger.info(f"Constitution successfully saved to: {output_file}")
            return True

        except Exception as e:
            logger.error(f"Scraping failed: {e}", exc_info=True)
            self._update_stats(errors=self.stats["errors"] + 1)
            return False
            
        finally:
            self._update_stats(end_time=datetime.utcnow())
            self._log_stats()

    async def verify_structure(self, output_file: str) -> bool:
        """
        Verify the structure of a saved constitution file
        
        Args:
            output_file: Path to the JSON file
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if not Path(output_file).exists():
                logger.error(f"File not found: {output_file}")
                return False

            # Load and validate JSON
            with open(output_file, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    return self.schema_validator.validate_data(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON format: {e}")
                    return False
            
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return False