# Crypto-Ingestion-Pipeline

A containerized ETL pipeline for real-time cryptocurrency market data.

## Architecture
- **Ingestion:** Python-based API client (Binance/Kraken)
- **Orchestration:** Airflow / Dagster
- **Transformation:** dbt (SQL-based modeling)
- **Storage:** PostgreSQL (Bronze/Gold layers)
- **Infrastructure:** Docker & WSL 2