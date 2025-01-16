"""
Main entry point for constitution scraping.
Created by: gabes-machado
Date: 2025-01-16 20:26:35
"""

import asyncio
import logging
from pathlib import Path
from datetime import datetime, UTC

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with current UTC time
    log_filename = f"scraper_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    log_path = log_dir / log_filename
    
    # Configure logging
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
    try:
        # Setup logging
        logger = setup_logging()
        
        # Create data directory if it doesn't exist
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        
        # Initialize scraper
        from scraper.constitution import ConstitutionScraper
        scraper = ConstitutionScraper()
        
        # Execute scraping
        output_file = data_dir / "constitution.json"
        await scraper.scrape(str(output_file))
        
        logger.info("Scraping completed successfully")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())