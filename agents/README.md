Datapipeline/
├── agents/
│   ├── nifty50_agent.py
│   ├── config.py
│   ├── requirements.txt
│   └── README.md
├── data/
│   └── nifty50_data.csv
└── logs/


# 1. Clone and navigate
git clone https://github.com/Amit1092tiwari/Datapipeline.git
cd Datapipeline

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate

# 3. Install dependencies
python -m pip install -r agents\requirements.txt or pip install -r agents/requirements.txt

# 4. Run from VS Code debugger (F5)
# Or from terminal:
python agents/nifty50_agent.py once
python agents/nifty50_agent.py continuous every_5_min