
# 📦 MongoDB to Snowflake Pipeline with Dagster + dlt
This project demonstrates how to build a robust, reusable data pipeline to extract data from MongoDB, transform it using dlt, and load it into Snowflake — all orchestrated via Dagster.

### 🚀 Tech Stack
Dagster: Workflow orchestration

dlt: ELT library for modern pipelines

MongoDB: Source database

Snowflake: Destination data warehouse

Python

### 📁 Project Structure
bash
Copy
Edit
dagster_ml/
│
├── mongodb/                # MongoDB source definition
├── assets/                 # Dagster assets
├── dagster_ml_tests/       # Tests
├── venv/                   # Virtual environment
├── setup.py
└── pyproject.toml

### ⚙️ How It Works
Connects to the sample_mflix MongoDB dataset using dlt.

Defines a Dagster asset using @dlt_assets which wraps the dlt pipeline.

Runs the pipeline with write disposition set to "merge", allowing idempotent Snowflake updates.


## ✅ Highlights

- Asset-based orchestration (Dagster)
- Modular source integration (dlt)
- Cloud data warehouse: Snowflake
- Trackable, reproducible pipelines
- Python-native, testable setup

## 💼 Skills Demonstrated

- Snowflake data ingestion  
- Workflow orchestration via Dagster  
- ELT pipeline design  
- Python packaging and project hygiene  
- Environment configuration and secure credential handling  


## 🧰 Project Setup

1. **Clone the repo:**
   ```bash
   git clone https://github.com/Konzisam/mongodb-to-snowflake.git
   cd mongodb-to-snowflake

bash
2. **Create virtual environment:**
git clone https://github.com/Konzisam/mongodb-to-snowflake.git
cd mongodb-to-snowflake
Create a virtual environment
bash `python -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
`
3. Install dependencies:
```pip install -e .```

bash
Copy
Edit
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
Install dependencies

##  ✅ Features
###ä#🧩 Modular asset-based design with Dagster

🔁 Idempotent loads using dlt’s merge disposition

🔒 Secure credentials via env vars

🌐 Cloud-ready (Snowflake support)

📚 Learning Goals
Showcase ability to integrate modern ELT tools in production-style pipelines

Understand asset-based orchestration using Dagster

Implement data ingestion and schema evolution

🧠 Future Enhancements
Add more MongoDB collections

Schedule pipeline runs

Integrate data validation

## Environment variables
bash ```SOURCES__MONGODB__MONGODB__CONNECTION_URL=${mongo_url}
DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE=${snowflake_database}
DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD=${snowflake_password}
DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME=${snowflake_user}
DESTINATION__SNOWFLAKE__CREDENTIALS__HOST=${snowflake_account}
DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE=${snowflake_warehouse}
DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE=${snowflake_role}

SNOWFLAKE_ACCOUNT=${snowflake_account}
SNOWFLAKE_USER=${snowflake_user}
SNOWFLAKE_PASSWORD=${snowflake_password}
```

📚 Learning Goals
Showcase ability to integrate modern ELT tools in production-style pipelines

Understand asset-based orchestration using Dagster

Implement data ingestion and schema evolution

🧠 Future Enhancements
Add more MongoDB collections

Schedule pipeline runs

Integrate data validation