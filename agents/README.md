# Market Telemetry Ingestion Agent (NSE Nifty 50)

Market data telemetry ingestion service for extracting live/calibrated NIFTY 50 price feeds and streaming them to lakehouse storage.

```text
open-lakehouse-platform/
├── agents/
│   ├── nifty50_agent.py
│   ├── config.py
│   ├── requirements.txt
│   └── README.md
├── data/
│   └── nifty50_data.csv
└── logs/
```

## Quick Start

### 1. Clone and navigate
```bash
git clone https://github.com/Amit1092tiwari/open-lakehouse-platform.git
cd open-lakehouse-platform
```

### 2. Create virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r agents/requirements.txt
```

### 4. Run ingestion
```bash
# Single execution (used by CI/CD quality gate)
python agents/nifty50_agent.py once

# Continuous streaming mode (default: every 5 min)
python agents/nifty50_agent.py continuous every_5_min
```
