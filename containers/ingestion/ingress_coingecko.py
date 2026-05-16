import os
import sys
import time
import json
import requests
from datetime import datetime

# Configuration
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
# Fallback to demo endpoint if a demo key is present
if os.getenv("COINGECKO_API_KEY"):
    COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3" 
    # For demo keys, CoinGecko often uses: https://api.coingecko.com/api/v3
    # For paid Pro keys, use: https://pro-api.coingecko.com/api/v3

TARGET_COINS = ["bitcoin", "ethereum", "solana"]
VS_CURRENCIES = "usd"

def fetch_crypto_data():
    """Fetches real-time market data from CoinGecko API."""
    endpoint = f"{COINGECKO_BASE_URL}/simple/price"
    params = {
        "ids": ",".join(TARGET_COINS),
        "vs_currencies": VS_CURRENCIES,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true"
    }
    
    headers = {
        "accept": "application/json"
    }
    
    # Inject API Key if defined in environment variables
    api_key = os.getenv("COINGECKO_API_KEY")
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=10)
        
        # Handle Rate Limiting (HTTP 429)
        if response.status_code == 429:
            print("Warning: Rate limit hit. Sleeping for 60 seconds...", file=sys.stderr)
            time.sleep(60)
            return None
            
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from CoinGecko: {e}", file=sys.stderr)
        return None

def format_pipeline_payload(raw_data):
    """Transforms the raw API layout into a structured ETL record."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    records = []
    
    for coin_id, metrics in raw_data.items():
        record = {
            "ingest_timestamp": timestamp,
            "asset_id": coin_id,
            "price_usd": metrics.get("usd"),
            "market_cap_usd": metrics.get("usd_market_cap"),
            "volume_24h_usd": metrics.get("usd_24h_vol"),
            "change_24h_pct": metrics.get("usd_24h_change"),
            "api_last_updated": datetime.utcfromtimestamp(metrics.get("last_updated_at")).isoformat() + "Z" if metrics.get("last_updated_at") else None
        }
        records.append(record)
        
    return {
        "source": "coingecko",
        "data": records
    }

def main():
    print("Starting CoinGecko data ingestion task...")
    raw_payload = fetch_crypto_data()
    
    if not raw_payload:
        print("Ingestion failed or returned no data.")
        sys.exit(1)
        
    processed_data = format_pipeline_payload(raw_payload)
    
    # Print schema payload to stdout for pipeline streaming or log tracking
    print(json.dumps(processed_data, indent=2))
    
    # Optional: Save locally to a landing zone file
    # output_path = f"data_landing/raw_{int(time.time())}.json"
    # os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # with open(output_path, "w") as f:
    #     json.dump(processed_data, f)

if __name__ == "__main__":
    main()