🛡️ AI Compliance & Audit Agent: MVP-AGENT /br
🌟 Project Overview: Autonomous AML Compliance
Financial institutions face massive manual effort and high error rates in monitoring for AML/KYC and trading rule violations.

Our solution, the AI Compliance & Audit Agent, is an autonomous system designed to ingest transaction data, automatically run complex compliance checks (both rule-based and anomaly detection), flag suspicious records, and generate an auditable report. This demonstrates how AI agents can transform manual compliance workflows into autonomous intelligence.

🎯 Goal & Key Features
The primary goal is to provide a clean, simple MVP where an auditor can upload a dataset, and the agent flags risks and generates a report within minutes.

Feature	Description	Technical Agent / Workflow
Data Ingestion	Ingests mock trade/transaction logs from a CSV file (data/sample.csv).	Ingestion Agent
Rule-Based Checks	Automatically runs compliance checks for specific rules (e.g., large transactions, missing client information, cross-border flows).	Rules Agent (src/rules.py using config/rules.yaml)
Anomaly Detection	Applies Machine Learning (ML) techniques to flag records that deviate significantly from normal patterns.	Anomaly Agent (src/anomaly.py)
Report Generation	Auto-generates a concise, natural language compliance report for managers and auditors to review.	Reporting Agent (src/report.py and a Large Language Model (LLM) for Natural Language Generation)

Export to Sheets
🚀 Getting Started (Run the Demo)
Follow these steps to get the project running locally.

1. Prerequisites
You'll need Python 3.9+ and Git installed.

2. Clone the Repository
Bash

git clone https://github.com/Jas-DEVOPS-2403/MVP-Agent.git
cd MVP-Agent
3. Setup Virtual Environment
It's crucial to isolate dependencies by using a virtual environment.

Bash

# Create the virtual environment (on all OS)
python -m venv .venv

# Activate the environment (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate the environment (macOS/Linux Bash)
# source .venv/bin/activate
4. Install Dependencies
All required libraries are listed in the requirements.txt file.

Bash

pip install -r requirements.txt
5. Run the Agent
The entire compliance workflow is orchestrated through the main.py file.

Bash

python main.py
Upon completion, the agent will:

Process data/sample.csv.

Apply rules from config/rules.yaml.

Perform anomaly detection.

Output the final report to the console or an output file.

📁 Project Structure
This is a high-level overview of the key files and directories:

File/Directory	Purpose
main.py	The main entry point; orchestrates the flow between all agents.
app.py	[Optional, if you add a web interface]: Flask/Streamlit application entry.
requirements.txt	Lists all Python dependencies for easy setup.
config/	Contains configuration files, notably rules.yaml for compliance thresholds.
data/	Contains sample data (sample.csv) and feedback logs (feedback.csv).
src/	Core logic for the AI Agents:
src/rules.py	Implements the deterministic compliance checks.
src/anomaly.py	Implements the ML model for detecting unusual behavior.
src/report.py	Responsible for collating findings and generating the final narrative report.
.gitattributes	Ensures consistent LF line endings across Windows and macOS development environments.
.gitignore	Excludes the virtual environment (.venv/) and other build files from Git.


💡 Innovation: Agentic Architecture
The project demonstrates innovation by using a Multi-Agent System that combines rules-based logic with ML-based anomaly detection. This modular design allows:

Separation of Concerns: Each agent is responsible for a specific compliance task (ingestion, feature creation, rule checking, reporting).

Flexibility: Compliance thresholds (rules) can be updated in config/rules.yaml without changing the core agent code.

Technical Depth: It integrates tabular anomaly detection, feature engineering, and Natural Language Generation (NLG) for the report.
