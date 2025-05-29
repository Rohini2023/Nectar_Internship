

# import requests
# import json
# import os
# from dotenv import load_dotenv

# load_dotenv()

# # Configuration
# MOCK_API_URL = "http://localhost:8080/platform-asset-1.0.0/latest/filter/access"
# MOCK_API_TOKEN = os.getenv("MOCK_API_TOKEN")

# def fetch_and_filter_assets():
#     # Prepare request
#     payload = {
#         "domain": "lremcofc",
#         "offset": 1,
#         "pageSize": 100
#     }

#     headers = {
#         'Authorization': f'Bearer {MOCK_API_TOKEN}',
#         'Content-Type': 'application/json'
#     }

#     try:
#         # 1. Make API request
#         response = requests.post(
#             MOCK_API_URL,
#             json=payload,
#             headers=headers,
#             timeout=5
#         )
#         response.raise_for_status()  # Raises exception for 4XX/5XX responses

#         print(f"Status Code: {response.status_code}")
        
#         # 2. Parse response
#         response_data = response.json()
#         print(f"Full response: {json.dumps(response_data, indent=2)}")
        
#         # 3. Extract assets (with error handling)
#         assets = response_data.get('data', {}).get('assets', [])
#         if not isinstance(assets, list):
#             raise ValueError("Invalid assets format in response")

#         # 4. Filter assets
#         filtered_assets = [
#             {
#                 "assetName": asset["displayName"],
#                 "thingId": asset.get("thingCode", ""),  # Using .get() for safety
#                 "displayName": asset["displayName"],
#                 "operationStatus": asset["operationStatus"]
#             }
#             for asset in assets
#             if (asset.get("operationStatus") == "ACTIVE" and 
#                 asset.get("displayName", "").startswith('F'))
#         ]

#         # 5. Prepare final output
#         result = {
#             "success": True,
#             "data": {
#                 "assets": filtered_assets,
#                 "total": len(filtered_assets),
#                 "original_total": response_data.get('data', {}).get('total', 0)
#             }
#         }

#         print(f"Filtered assets: {json.dumps(result, indent=2)}")
#         return result

#     except requests.exceptions.RequestException as e:
#         error_msg = f"API request failed: {str(e)}"
#         print(error_msg)
#         return {"success": False, "error": error_msg}
#     except (KeyError, ValueError) as e:
#         error_msg = f"Data processing error: {str(e)}"
#         print(error_msg)
#         return {"success": False, "error": error_msg}

# # Execute the function
# if __name__ == "__main__":
#     fetch_and_filter_assets()




import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
MOCK_API_URL = "http://localhost:8080/platform-asset-1.0.0/latest/filter/access"
MOCK_API_TOKEN = os.getenv("MOCK_API_TOKEN")

def fetch_and_filter_assets():
    # Prepare request with additional filters
    payload = {
        "domain": "lremcofc",
        "offset": 1,
        "pageSize": 100,
        # Add filters at API level if supported
        "operationStatus": ["ACTIVE", "Running"],
        "communicationStatus": ["COMMUNICATING"]
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
            timeout=10  # Increased timeout for reliability
        )
        response.raise_for_status()

        # 2. Parse response
        response_data = response.json()
        assets = response_data.get('data', {}).get('assets', [])
        
        if not isinstance(assets, list):
            raise ValueError("Invalid assets format in response")

        # 3. Filter assets with comprehensive checks
        filtered_assets = []
        for asset in assets:
            try:
                # Essential field validation
                if not all(key in asset for key in ["thingCode", "displayName", "operationStatus", "communicationStatus"]):
                    continue

                # Business logic filtering
                if (asset["operationStatus"] in ["ACTIVE", "Running"] and
                    asset["communicationStatus"] == "COMMUNICATING" and
                    asset.get("thingCode")):
                    
                    # Get time reference (prefer communicatingTimes, fallback to createdOn)
                    time_reference = (
                        asset.get("communicatingTimes", {}).get("endDate") 
                        or asset.get("createdOn")
                    )

                    filtered_assets.append({
                        "thingId": asset["thingCode"],
                        "displayName": asset["displayName"],
                        "assetType": asset.get("type"),
                        "timeReference": time_reference,
                        "operationStatus": asset["operationStatus"],
                        "communicationStatus": asset["communicationStatus"]
                    })
            except KeyError as e:
                print(f"Skipping asset due to missing field: {e}")
                continue

        # 4. Prepare final output
        result = {
            "success": True,
            "data": {
                "assets": filtered_assets,
                "filtered_count": len(filtered_assets),
                "original_count": len(assets),
                "time_retrieved": int(time.time() * 1000)  # Current timestamp in ms
            }
        }

        print(f"Filtered {len(filtered_assets)}/{len(assets)} assets")
        return result

    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        print(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "retryable": isinstance(e, (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError
            ))
        }
    except (ValueError, KeyError, json.JSONDecodeError) as e:
        error_msg = f"Data processing error: {str(e)}"
        print(error_msg)
        return {"success": False, "error": error_msg}

if __name__ == "__main__":
    import time
    start_time = time.time()
    result = fetch_and_filter_assets()
    print(f"Execution time: {time.time() - start_time:.2f}s")
    print(json.dumps(result, indent=2))
