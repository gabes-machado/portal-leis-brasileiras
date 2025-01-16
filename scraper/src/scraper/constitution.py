"""
Main constitution scraper implementation.
Author: gabes-machado
Created: 2025-01-16 20:34:14 UTC
"""

import logging
from typing import Optional

from utils.http_client import AsyncHTTPClient
from utils.html_parser import HTMLParser
from utils.data_transformer import ConstitutionTransformer
from utils.json_handler import JSONHandler

logger = logging.getLogger(__name__)

class ConstitutionScraper:
    def __init__(self, base_url: str = "https://www.planalto.gov.br"):
        self.base_url = base_url
        self.constitution_path = "/ccivil_03/constituicao/constituicao.htm"
        self.transformer = ConstitutionTransformer()

    async def scrape(self, output_file: str) -> bool:
        """
        Execute the complete scraping process
        
        Returns:
            bool: True if scraping was successful, False otherwise
        """
        try:
            # Fetch HTML
            html_content = await self._fetch_html()
            if not html_content:
                logger.error("Failed to fetch HTML content")
                return False

            # Parse HTML
            parser = HTMLParser(html_content)
            parser.remove_strike_tags()
            paragraphs = parser.get_paragraphs()

            if not paragraphs:
                logger.error("No paragraphs found in HTML content")
                return False

            # Transform data
            df = self.transformer.create_dataframe(paragraphs)
            df = self.transformer.extract_hierarchical_structure(df)

            # Convert to nested structure and save
            nested_dict = JSONHandler.df_to_nested_dict(df)
            JSONHandler.save_json(nested_dict, output_file)

            logger.info(f"Constitution successfully saved to: {output_file}")
            return True

        except Exception as e:
            logger.error(f"Error during scraping process: {e}")
            return False

    async def _fetch_html(self) -> Optional[str]:
        """Fetch HTML content from Planalto website"""
        url = f"{self.base_url}{self.constitution_path}"
        
        async with AsyncHTTPClient() as client:
            try:
                return await client.get(url)
            except Exception as e:
                logger.error(f"Failed to fetch constitution: {e}")
                return None