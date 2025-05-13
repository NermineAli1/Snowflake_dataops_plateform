import os
import logging
import requests
from typing import List, Dict, Any
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables from .env (API keys, etc.)
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simulated endpoint or Google Ads API
GOOGLE_ADS_ENDPOINT = ""
API_KEY = os.getenv("GOOGLE_ADS_API_KEY") 

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_ads_data_from_api() -> List[Dict[str, Any]]:
    """
    Simulated API call to Google Ads or equivalent. This would typically use a client SDK.
    """
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    logger.info("Fetching Google Ads data from API...")

    try:
        response = requests.get(GOOGLE_ADS_ENDPOINT, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an error on non-2xx
        data = response.json()

        if not isinstance(data, list):
            raise ValueError("Unexpected response format: expected a list of dicts")

        logger.info(f"Successfully fetched {len(data)} records.")
        return data

    except requests.RequestException as e:
        logger.error(f"Error during API request: {e}")
        raise

    except ValueError as e:
        logger.error(f"Data format error: {e}")
        raise


def extract_google_ads_data() -> List[Dict[str, Any]]:
    """
    Wrapper function used by Airflow or external pipeline.
    Could include data caching, schema checks, etc.
    """
    try:
        raw_data = fetch_ads_data_from_api()
        logger.debug(f"First row test: {raw_data[0] if raw_data else 'No data'}")
        return raw_data

    except Exception as e:
        logger.exception("Failed to extract Google Ads data")
        return []