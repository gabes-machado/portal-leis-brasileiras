"""
Main entry point for constitution scraping.
Author: gabes-machado
Created: 2025-01-16 20:34:14 UTC
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime, UTC

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_filename = f"scraper_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    log_path = log_dir / log_filename
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_path))
        ]
    )

    return logging.getLogger(__name__)

async def main():
    logger = setup_logging()
    
    try:
        # Create data directory
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        
        # Initialize scraper
        from scraper.constitution import ConstitutionScraper
        scraper = ConstitutionScraper()
        
        # Execute scraping
        output_file = data_dir / "constitution.json"
        success = await scraper.scrape(str(output_file))
        
        if success:
            logger.info("Scraping completed successfully")
            sys.exit(0)
        else:
            logger.error("Scraping failed")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Scraping failed with unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())