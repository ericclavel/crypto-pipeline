import os
import sys
import json
import logging
import requests
from datetime import datetime
from requests.exceptions import ConnectionError, Timeout, HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging to track retry behavior cleanly
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuration
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
if os.getenv("COINGECKO_API_KEY"):
    COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3" 

TARGET_COINS = ["bitcoin", "ethereum", "solana"]
VS_CURRENCIES = "usd"


def is_transient_error(exception):
    """Filter to determine if the exception warrants a retry attempt."""
    if isinstance(exception, (ConnectionError, Timeout)):
        return True
    if isinstance(exception, HTTPError):
        status_code = exception.response.status_code
        # Retry on Rate Limits (429) or standard server errors (5xx)
        return status_code == 429 or 500 <= status_code < 600
    return False


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    retry=is_transient_error,
    before_sleep=lambda retry_state: logger.warning(
        f"CoinGecko API call failed (Attempt {retry_state.attempt_number}). "
        f"Retrying in {retry_state.next_action.sleep:.2f} seconds..."
    ),
    reraise=True  # Allows the final exception to bubble up if all 5 attempts fail
)
def fetch_crypto_data():
    """Fetches real-time market data from CoinGecko API with exponential backoff."""
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
    
    api_key = os.getenv("COINGECKO_API_KEY")
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    response = requests.get(endpoint, params=params, headers=headers, timeout=10)
    
    # This replaces manual status checking; triggers HTTPError for 429s/500s
    response.raise_for_status()
    
    return response.json()


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
    logger.info("Starting CoinGecko data ingestion task...")
    try:
        raw_payload = fetch_crypto_data()
        processed_data = format_pipeline_payload(raw_payload)
        print(json.dumps(processed_data, indent=2))
        
    except Exception as e:
        logger.error(f"Ingestion critical failure: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()