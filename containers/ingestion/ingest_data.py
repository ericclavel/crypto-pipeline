import os
import time
import json
import psycopg2
from psycopg2.extras import execute_values  # Optimized batch loader
from datetime import datetime
from pydantic import ValidationError

from ingress_coingecko import fetch_crypto_data 
from schemas.crypto_model import CryptoPriceData

# Configuration
DB_HOST = os.getenv("DB_HOST", "db") 
DB_NAME = os.getenv("DB_NAME", "crypto_data")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")
DLQ_DIR = os.getenv("DLQ_DIR", "data/dlq")

os.makedirs(DLQ_DIR, exist_ok=True)

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    except Exception as e:
        print(f"[CRITICAL] Database connection failed: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if not conn:
        return

    # Using context managers automatically closes the cursor and commits the transaction
    with conn:
        with conn.cursor() as cur:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                coin_id VARCHAR(50) NOT NULL,
                price_usd DECIMAL(18, 8) NOT NULL,
                last_updated_at TIMESTAMP NOT NULL,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cur.execute(create_table_query)
    conn.close()
    print("Database schema initialized.")

def route_to_dlq(coin_id: str, raw_details: any, error_message: str):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"failed_{coin_id}_{timestamp}.json"
    filepath = os.path.join(DLQ_DIR, filename)
    
    dlq_payload = {
        "pipeline_metadata": {
            "failed_at": datetime.utcnow().isoformat(),
            "validation_error": error_message
        },
        "raw_data": {
            "coin_id": coin_id,
            "details": raw_details
        }
    }
    
    with open(filepath, "w") as f:
        json.dump(dlq_payload, f, indent=4)
    print(f"[DLQ ALERT] Malformed data for '{coin_id}' logged to {filepath}")

def validate_and_transform(payload: dict) -> list[tuple]:
    """Validates data via Pydantic and outputs clean tuples ready for psycopg2 execute_values."""
    if not payload or not isinstance(payload, dict):
        print("Empty or invalid root payload shape. Skipping processing block.")
        return []

    clean_records = []

    for coin_id, details in payload.items():
        if not isinstance(details, dict):
            route_to_dlq(coin_id, details, "Root payload child node is not a valid dictionary structure.")
            continue

        try:
            validated_record = CryptoPriceData(
                asset_id=coin_id,
                price_usd=details.get("usd"),
                last_updated=details.get("last_updated_at")
            )
            
            # Map directly to a tuple alignment matching the DB column structure
            clean_records.append((
                validated_record.asset_id,
                validated_record.price_usd,
                validated_record.last_updated
            ))

        except ValidationError as e:
            route_to_dlq(coin_id, details, e.errors())
        except Exception as e:
            route_to_dlq(coin_id, details, f"Unexpected anomaly: {str(e)}")

    return clean_records

def insert_data(validated_records: list[tuple]):
    """Loads records atomically into PostgreSQL via optimized multi-row execution."""
    if not validated_records:
        return

    conn = get_db_connection()
    if not conn:
        return
        
    insert_query = """
    INSERT INTO crypto_prices (coin_id, price_usd, last_updated_at)
    VALUES %s;
    """
    
    try:
        # The 'with conn' block manages the transaction. 
        # If an error happens inside, it auto-rolls back the entire batch.
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, validated_records)
        print(f"Successfully committed batch of {len(validated_records)} records to database.")
    except Exception as e:
        print(f"[BATCH ERROR] Transaction rolled back. Insert failed for entire batch: {e}")
    finally:
        conn.close()

def main():
    print("Starting ingestion engine...")
    init_db()
    
    while True:
        print("Fetching data from CoinGecko script...")
        try:
            raw_data = fetch_crypto_data()
            if raw_data:
                valid_batch = validate_and_transform(raw_data)
                if valid_batch:
                    insert_data(valid_batch)
                else:
                    print("Batch contains zero valid entries to commit to database.")
            else:
                print("No data received from fetch function.")
        except Exception as e:
            print(f"[PIPELINE RUNTIME ERROR] Anomaly caught in execution loop: {e}")
            
        print("Waiting 60 seconds for next poll...")
        time.sleep(60)

if __name__ == "__main__":
    main()