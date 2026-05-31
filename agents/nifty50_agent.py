import os
import sys
import csv
import json
import time
import logging
from datetime import datetime
from pathlib import Path
import urllib.request
import urllib.error

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agents.config import (
    NSE_CONFIG,
    DATA_CONFIG,
    FREQUENCY_CONFIG,
    LOGGING_CONFIG,
    WINDOWS_CONFIG,
)

# Configure logging
logging.basicConfig(
    level=LOGGING_CONFIG["log_level"],
    format=LOGGING_CONFIG["format"],
    handlers=[
        logging.FileHandler(LOGGING_CONFIG["log_file"], encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class NiftyNSEAgent:
    """Agent for ingesting NSE Nifty 50 data into CSV"""

    def __init__(self):
        self.api_url = NSE_CONFIG["api_url"]
        self.index = NSE_CONFIG["index"]
        self.timeout = NSE_CONFIG["timeout"]
        self.csv_file = DATA_CONFIG["csv_file"]
        self.file_exists = self.csv_file.exists()
        logger.info(f"Agent initialized. CSV file path: {self.csv_file}")

    def fetch_nifty_data(self):
        """Fetch NSE Nifty 50 data using only stdlib"""
        try:
            url = f"{self.api_url}?index={self.index}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                data = response.read().decode("utf-8")
                return json.loads(data)

        except urllib.error.URLError as e:
            logger.error(f"Network error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            return None

    def parse_nifty_data(self, raw_data):
        """Parse and extract relevant fields"""
        if not raw_data or "data" not in raw_data or not raw_data["data"]:
            logger.warning("No data in response")
            return None

        nifty_info = raw_data["data"][0]

        parsed_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "index_name": nifty_info.get("index", "NIFTY 50"),
            "current_value": nifty_info.get("last", "N/A"),
            "change": nifty_info.get("net", "N/A"),
            "change_percent": nifty_info.get("percnt", "N/A"),
            "high": nifty_info.get("high", "N/A"),
            "low": nifty_info.get("low", "N/A"),
            "open": nifty_info.get("open", "N/A"),
            "close": nifty_info.get("close", "N/A"),
        }

        logger.debug(f"Parsed data: {parsed_data}")
        return parsed_data

    def save_to_csv(self, data):
        """Save data to CSV file (Windows-friendly)"""
        if not data:
            logger.warning("No data to save")
            return False

        try:
            fieldnames = list(data.keys())

            # Write with Windows-appropriate settings
            with open(
                self.csv_file,
                "a" if self.file_exists else "w",
                newline="",
                encoding="utf-8-sig",  # BOM for Excel
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                # Write header only for new file
                if not self.file_exists:
                    writer.writeheader()
                    self.file_exists = True

                writer.writerow(data)

            logger.info(f"✓ Data saved to {self.csv_file}")
            return True

        except IOError as e:
            logger.error(f"File I/O error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return False

    def run_once(self):
        """Single execution"""
        logger.info("=" * 60)
        logger.info("Starting Nifty 50 data ingestion...")
        logger.info("=" * 60)

        raw_data = self.fetch_nifty_data()
        if raw_data:
            parsed_data = self.parse_nifty_data(raw_data)
            if parsed_data:
                self.save_to_csv(parsed_data)
                logger.info("Ingestion completed successfully")
                return True
        return False

    def run_continuous(self, frequency_key="every_5_min"):
        """Run continuously at specified frequency"""
        frequency = FREQUENCY_CONFIG.get(frequency_key, 300)
        logger.info(f"Starting continuous ingestion (frequency: {frequency_key} = {frequency}s)")

        iteration = 0
        try:
            while True:
                iteration += 1
                logger.info(f"\n--- Iteration {iteration} ---")
                self.run_once()
                logger.info(f"Next run in {frequency} seconds... (Press Ctrl+C to stop)")
                time.sleep(frequency)

        except KeyboardInterrupt:
            logger.info("\n✓ Agent stopped by user")
        except Exception as e:
            logger.error(f"Error in continuous run: {e}")

    def get_csv_stats(self):
        """Display CSV statistics"""
        if not self.csv_file.exists():
            logger.info("CSV file not yet created")
            return

        try:
            with open(self.csv_file, "r", encoding="utf-8-sig") as f:
                lines = f.readlines()
                logger.info(f"CSV file stats: {len(lines) - 1} records (excluding header)")
                logger.info(f"File location: {self.csv_file.absolute()}")
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")


def main():
    """CLI Interface"""
    agent = NiftyNSEAgent()

    if len(sys.argv) > 1:
        command = sys.argv[1].lower()

        if command == "once":
            agent.run_once()

        elif command == "continuous":
            frequency = sys.argv[2] if len(sys.argv) > 2 else "every_5_min"
            agent.run_continuous(frequency)

        elif command == "stats":
            agent.get_csv_stats()

        else:
            print_help()
    else:
        print_help()


def print_help():
    """Print help message"""
    help_text = """
    NSE Nifty 50 Data Ingestion Agent - Windows Version
    ====================================================
    
    Usage:
        python nifty50_agent.py <command> [frequency]
    
    Commands:
        once                    Run once and exit
        continuous [frequency]  Run continuously
        stats                   Show CSV statistics
    
    Frequency options (for continuous mode):
        daily           Every 24 hours
        hourly          Every 1 hour
        every_15_min    Every 15 minutes
        every_5_min     Every 5 minutes (default)
        every_min       Every 1 minute
        high_freq       Every 10 seconds (testing)
    
    Examples:
        python nifty50_agent.py once
        python nifty50_agent.py continuous every_5_min
        python nifty50_agent.py continuous hourly
        python nifty50_agent.py stats
    """
    print(help_text)


if __name__ == "__main__":
    main()