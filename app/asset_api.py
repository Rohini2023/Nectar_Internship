import requests
import os
import logging
from dotenv import load_dotenv

load_dotenv()

ASSET_API_URL = os.getenv("ASSET_API_URL")

logger = logging.getLogger(__name__)

def fetch_assets(token):
    headers = {"Authorization": f"Bearer {token}"}
    assets = []
    page = 1
    page_size = 100

    while True:
        try:
            response = requests.get(
                f"{ASSET_API_URL}?page={page}&page_size={page_size}",
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            page_assets = data.get("assets", [])
            if not page_assets:
                break
            assets.extend(page_assets)
            page += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch assets: {e}")
            break

    return assets
