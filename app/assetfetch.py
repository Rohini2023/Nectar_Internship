# import requests
# import os
# import logging
# from dotenv import load_dotenv

# load_dotenv()

# ASSET_API_URL = os.getenv("ASSET_API_URL")

# logger = logging.getLogger(__name__)

# def fetch_assets(token):
#     headers = {"Authorization": f"Bearer {token}"}
#     assets = []
#     page = 1
#     page_size = 100

#     while True:
#         try:
#             response = requests.get(
#                 f"{ASSET_API_URL}?page={page}&page_size={page_size}",
#                 headers=headers,
#                 timeout=10
#             )
#             response.raise_for_status()
#             data = response.json()
#             page_assets = data.get("assets", [])
#             if not page_assets:
#                 break
#             assets.extend(page_assets)
#             page += 1
#         except requests.exceptions.RequestException as e:
#             logger.error(f"Failed to fetch assets: {e}")
#             break

#     return assets


import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
MOCK_API_URL = "http://localhost:8080/platform-asset-1.0.0/latest/filter/access"
MOCK_API_TOKEN = os.getenv("MOCK_API_TOKEN")

def fetch_and_filter_assets():
    # Prepare request
    payload = {
        "domain": "lremcofc",
        "offset": 1,
        "pageSize": 100
    }

    headers = {
        'Authorization': f'Bearer {MOCK_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    try:
        # 1. Make API request
        response = requests.post(
            MOCK_API_URL,
            json=payload,
            headers=headers,
            timeout=5
        )
        response.raise_for_status()  # Raises exception for 4XX/5XX responses

        print(f"Status Code: {response.status_code}")
        
        # 2. Parse response
        response_data = response.json()
        print(f"Full response: {json.dumps(response_data, indent=2)}")
        
        # 3. Extract assets (with error handling)
        assets = response_data.get('data', {}).get('assets', [])
        if not isinstance(assets, list):
            raise ValueError("Invalid assets format in response")

        # 4. Filter assets
        filtered_assets = [
            {
                "assetName": asset["displayName"],
                "thingId": asset.get("thingCode", ""),  # Using .get() for safety
                "displayName": asset["displayName"],
                "operationStatus": asset["operationStatus"]
            }
            for asset in assets
            if (asset.get("operationStatus") == "ACTIVE" and 
                asset.get("displayName", "").startswith('F'))
        ]

        # 5. Prepare final output
        result = {
            "success": True,
            "data": {
                "assets": filtered_assets,
                "total": len(filtered_assets),
                "original_total": response_data.get('data', {}).get('total', 0)
            }
        }

        print(f"Filtered assets: {json.dumps(result, indent=2)}")
        return result

    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        print(error_msg)
        return {"success": False, "error": error_msg}
    except (KeyError, ValueError) as e:
        error_msg = f"Data processing error: {str(e)}"
        print(error_msg)
        return {"success": False, "error": error_msg}

# Execute the function
if __name__ == "__main__":
    fetch_and_filter_assets()


