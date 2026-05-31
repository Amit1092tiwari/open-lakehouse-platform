import os
from pathlib import Path

# Base configuration
BASE_DIR = Path(__file__).resolve().parent.parent
AGENTS_DIR = BASE_DIR / "agents"
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Create directories if they don't exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# NSE Configuration
NSE_CONFIG = {
    "api_url": "https://www.nseindia.com/api/index-data",
    "index": "NIFTY%2050",
    "timeout": 10,
}

# Data Configuration
DATA_CONFIG = {
    "csv_file": DATA_DIR / "nifty50_data.csv",
    "output_format": "csv",
    "append_mode": True,
}

# Frequency Configuration (in seconds)
FREQUENCY_CONFIG = {
    "daily": 86400,        # 24 hours
    "hourly": 3600,        # 1 hour
    "every_15_min": 900,   # 15 minutes
    "every_5_min": 300,    # 5 minutes
    "every_min": 60,       # 1 minute
    "high_freq": 10,       # 10 seconds (testing)
}

# Logging Configuration
LOGGING_CONFIG = {
    "log_file": LOGS_DIR / "nifty50_ingest.log",
    "log_level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

# Windows-specific settings
WINDOWS_CONFIG = {
    "use_colored_output": True,
    "use_native_paths": True,  # Use backslashes
    "encoding": "utf-8-sig",   # BOM for Excel compatibility
}