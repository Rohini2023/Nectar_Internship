import requests
import os
import logging
from dotenv import load_dotenv

load_dotenv()

AUTH_URL = os.getenv("AUTH_URL")
CLIENT_ID = os.getenv("AUTH_CLIENT_ID")
CLIENT_SECRET = os.getenv("AUTH_CLIENT_SECRET")

logger = logging.getLogger(__name__)

def get_auth_token():
    try:
        response = requests.post(
            AUTH_URL,
            json={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
            timeout=10
        )
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch token: {e}")
        return None
