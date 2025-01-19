"""
Main entry point for constitution scraping.
Author: gabes-machado
Created: 2025-01-17 01:52:27 UTC
"""

import asyncio
import logging
import sys
import signal
import json
from pathlib import Path
from datetime import datetime, UTC
from typing import Optional
import argparse
import platform

# Default configuration
DEFAULT_CONFIG = {
    "base_url": "https://www.planalto.gov.br",
    "max_retries": 3,
    "timeout": 30,
    "log_level": "INFO",
    "output_format": "json"
}

class ScraperApp:
    """Main application class for constitution scraper"""
    
    def __init__(self):
        self.logger = None
        self.config = DEFAULT_CONFIG.copy()
        self.start_time = None
        self.output_file = None
        self._shutdown_requested = False

    def setup_logging(self, log_level: str = "INFO") -> logging.Logger:
        """
        Setup logging configuration with rotation
        
        Args:
            log_level: Logging level to use
        """
        try:
            log_dir = Path(__file__).parent / "logs"
            log_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
            log_path = log_dir / f"scraper_{timestamp}.log"
            
            # Configure logging format
            log_format = (
                '%(asctime)s UTC - %(name)s - %(levelname)s - '
                '%(message)s'
            )
            
            # Set up handlers
            handlers = [
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(str(log_path), encoding='utf-8')
            ]
            
            # Configure logging
            logging.basicConfig(
                level=getattr(logging, log_level.upper()),
                format=log_format,
                handlers=handlers
            )
            
            logger = logging.getLogger(__name__)
            
            # Log system information
            logger.info(f"Python version: {sys.version}")
            logger.info(f"Platform: {platform.platform()}")
            logger.info(f"Log file: {log_path}")
            
            return logger
            
        except Exception as e:
            print(f"Failed to setup logging: {e}", file=sys.stderr)
            sys.exit(1)

    def load_config(self, config_path: Optional[Path] = None) -> None:
        """
        Load configuration from file or use defaults
        
        Args:
            config_path: Optional path to config file
        """
        try:
            if config_path and config_path.exists():
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                self.config.update(file_config)
                self.logger.info(f"Loaded configuration from {config_path}")
            else:
                self.logger.info("Using default configuration")
                
        except Exception as e:
            self.logger.error(f"Error loading config: {e}")
            self.logger.info("Falling back to default configuration")

    def setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers"""
        def signal_handler(signum, frame):
            sig_name = signal.Signals(signum).name
            self.logger.warning(f"Received signal {sig_name}")
            self._shutdown_requested = True
            
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, signal_handler)

    def create_directories(self) -> None:
        """Create necessary directories"""
        try:
            dirs = {
                'data': Path(__file__).parent / "data",
                'logs': Path(__file__).parent / "logs",
                'temp': Path(__file__).parent / "temp"
            }
            
            for name, path in dirs.items():
                path.mkdir(exist_ok=True)
                self.logger.debug(f"Created directory: {path}")
                
        except Exception as e:
            self.logger.error(f"Failed to create directories: {e}")
            raise

    async def run_scraper(self) -> bool:
        """
        Run the constitution scraper
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            from scraper.constitution import ConstitutionScraper
            
            scraper = ConstitutionScraper(
                base_url=self.config['base_url'],
                max_retries=self.config['max_retries'],
                timeout=self.config['timeout']
            )
            
            success = await scraper.scrape(str(self.output_file))
            
            if success:
                self.logger.info("Scraping completed successfully")
                # Verify the output
                if await scraper.verify_structure(str(self.output_file)):
                    self.logger.info("Output verification successful")
                else:
                    self.logger.error("Output verification failed")
                    return False
            else:
                self.logger.error("Scraping failed")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Error running scraper: {e}", exc_info=True)
            return False

    def parse_arguments(self) -> argparse.Namespace:
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(
            description='Brazilian Constitution Scraper'
        )
        parser.add_argument(
            '--config', 
            type=Path,
            help='Path to configuration file'
        )
        parser.add_argument(
            '--log-level',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
            default='INFO',
            help='Logging level'
        )
        parser.add_argument(
            '--output',
            type=Path,
            help='Output file path'
        )
        return parser.parse_args()

    async def run(self) -> int:
        """
        Main application entry point
        
        Returns:
            int: Exit code (0 for success, 1 for failure)
        """
        try:
            # Parse arguments
            args = self.parse_arguments()
            
            # Setup logging
            self.logger = self.setup_logging(args.log_level)
            
            # Load configuration
            self.load_config(args.config)
            
            # Setup signal handlers
            self.setup_signal_handlers()
            
            # Create directories
            self.create_directories()
            
            # Set output file
            data_dir = Path(__file__).parent / "data"
            self.output_file = args.output or (
                data_dir / "constitution.json"
            )
            
            # Record start time
            self.start_time = datetime.now(UTC)
            self.logger.info(f"Starting scraper at {self.start_time} UTC")
            
            # Run scraper
            success = await self.run_scraper()
            
            # Calculate duration
            duration = datetime.now(UTC) - self.start_time
            self.logger.info(
                f"Scraping finished in {duration.total_seconds():.2f} seconds"
            )
            
            return 0 if success else 1
            
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"Fatal error: {e}", 
                    exc_info=True
                )
            else:
                print(f"Fatal error: {e}", file=sys.stderr)
            return 1

def main():
    """Entry point function"""
    app = ScraperApp()
    try:
        exit_code = asyncio.run(app.run())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()