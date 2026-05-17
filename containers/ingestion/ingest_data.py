import os
import time
import psycopg2

# Adjust the import name below to match the actual function name in your ingress_coingecko.py file
from ingress_coingecko import fetch_crypto_data 

# Database Configuration (mapped to docker-compose.yml)
DB_HOST = os.getenv("DB_HOST", "db") 
DB_NAME = os.getenv("DB_NAME", "crypto_data")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASS = os.getenv("DB_PASS", "admin")

def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()
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
    conn.commit()
    cur.close()
    conn.close()
    print("Database schema initialized.")

def insert_data(payload):
    """Parses and inserts the direct coin-mapped dictionary structure from CoinGecko"""
    if not payload or not isinstance(payload, dict):
        print("Invalid payload format received: Payload is empty or not a dictionary.")
        return

    conn = get_db_connection()
    if not conn:
        return
        
    cur = conn.cursor()
    insert_query = """
    INSERT INTO crypto_prices (coin_id, price_usd, last_updated_at)
    VALUES (%s, %s, to_timestamp(%s));
    """
    
    # Loop over the root dictionary keys (e.g., 'bitcoin', 'ethereum', 'solana')
    for coin_id, details in payload.items():
        if isinstance(details, dict):
            price = details.get("usd")
            # Fallback to current time if last_updated_at is missing
            unix_timestamp = details.get("last_updated_at", time.time()) 
            
            if price is not None:
                try:
                    cur.execute(insert_query, (coin_id, price, unix_timestamp))
                    print(f"Successfully Inserted: {coin_id} at ${price}")
                except Exception as e:
                    print(f"Database insertion failed for {coin_id}: {e}")
            else:
                print(f"Skipping record for {coin_id}: 'usd' price missing. Data: {details}")
        else:
            print(f"Skipping invalid nested structure under key {coin_id}: {details}")

    conn.commit()
    cur.close()
    conn.close()

def main():
    print("Starting ingestion engine...")
    init_db() # Explicitly builds table on launch
    
    while True:
        print("Fetching data from CoinGecko script...")
        raw_data = fetch_crypto_data()
        
        if raw_data:
            insert_data(raw_data)
        else:
            print("No data received from fetch function.")
            
        print("Waiting 60 seconds for next poll...")
        time.sleep(60)

if __name__ == "__main__":
    main()