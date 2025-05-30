# Import dependencies
from app.cassandra_ops import connect_to_cassandra, get_earliest_log_date
from app.postgres_ops import connect_postgres, get_last_calculated_date
from app.assetfetch import fetch_and_filter_assets
from app.run_hour_calculation import process_asset_for_date
from datetime import date, datetime, timedelta
import sys
import logging
import argparse
import time  # For execution timing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    """
    Convert date string to date object with validation
    Args:
        date_str: Date in YYYY-MM-DD format
    Returns:
        date object
    Exits on invalid format
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
        sys.exit(1)

def get_date_range_from_args():
    """
    Parse command line arguments for date range processing
    Returns:
        tuple: (start_date, end_date, force_update)
        None values indicate default behavior should be used
    """
    parser = argparse.ArgumentParser(description='Process run hours for assets')
    parser.add_argument('dates', nargs='*', help='Date range to process (start_date end_date)')
    parser.add_argument('--force', action='store_true', help='Force reprocessing of all dates in range')
    args = parser.parse_args()

    yesterday = date.today() - timedelta(days=1)
    force_update = args.force

    # Handle different argument combinations
    if len(args.dates) == 0:
        return None, None, force_update  # Default to auto-range calculation
    elif len(args.dates) == 1:
        d = parse_date(args.dates[0])
        return d, d, force_update  # Single date mode
    elif len(args.dates) == 2:
        start = parse_date(args.dates[0])
        end = parse_date(args.dates[1])
        if end < start:
            logger.error("End date cannot be earlier than start date.")
            sys.exit(1)
        return start, end, force_update  # Date range mode
    else:
        logger.error("Invalid arguments. Usage: python main.py [start_date] [end_date] [--force]")
        sys.exit(1)

def handle_asset_fetching():
    """
    Fetch and validate assets with error handling
    Returns:
        dict: Returns full asset data structure on success, or fallback asset on failure
              Format: {
                  'success': bool,
                  'assets': list[dict],  # Full asset data when successful
                  'fallback_used': bool,  # True when using fallback
                  'error': str  # Only present on failure
              }
    """
    start_time = time.time()
    asset_result = fetch_and_filter_assets()
    
    # Prepare base response structure
    response = {
        'success': asset_result['success'],
        'fallback_used': False
    }
    
    # Handle API failure cases
    if not asset_result['success']:
        logger.error(f"Asset fetch failed: {asset_result['error']}")
        response['error'] = asset_result['error']
        response['fallback_used'] = True
        
        if asset_result.get('retryable', False):
            logger.warning("Retryable error - using fallback asset AC_001")
        else:
            logger.error("Non-retryable error - using fallback asset AC_001")
        
        # Return fallback with same structure as successful response
        response['assets'] = [{
            "thingId": "AC_001",
            "displayName": "Fallback Asset",
            "operationStatus": "ACTIVE",
            "communicationStatus": "COMMUNICATING",
            "TimeReference": 1746053925000,
            "is_fallback": True  # Mark as fallback
        }]
        return response
    
    # On success, return all asset data
    logger.info(f"Fetched {asset_result['data']['filtered_count']} assets in {time.time() - start_time:.2f}s")
    response['assets'] = asset_result['data']['assets']
    return response

def main():
    """Main execution flow for run hour calculation"""
    start_time = time.time()
    
    # Initialize database connections
    cassandra_session = connect_to_cassandra()
    pg_conn = connect_postgres()
    
    try:
        # 1. Parse command line arguments
        user_start, user_end, force_update = get_date_range_from_args()
        yesterday = date.today() - timedelta(days=1)

        # 2. Fetch assets to process
        asset_response = handle_asset_fetching()
        
        if not asset_response['success']:
            logger.warning(f"Using fallback assets due to: {asset_response.get('error', 'Unknown error')}")
        
        assets = asset_response['assets']
        logger.info(f"Processing {len(assets)} assets (fallback used: {asset_response['fallback_used']})")
        
        # 3. Process each asset
        for asset in assets:
            thingid = asset['thingId']
            logger.info(f"Processing {thingid} (force={force_update})")
            
            # Get createdOn date if available
            created_date = None
            if 'createdOn' in asset and asset['createdOn']:
                try:
                    created_date = datetime.fromtimestamp(asset['createdOn']/1000).date()
                    logger.debug(f"Asset {thingid} created on {created_date}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid createdOn timestamp for {thingid}: {e}")

            # Determine calculation range based on mode
            if force_update:
                if user_start is None:
                    calc_start = calc_end = yesterday
                else:
                    calc_start = user_start
                    calc_end = user_end or user_start
            else:
                last_calculated = get_last_calculated_date(pg_conn, thingid)
                last_date = last_calculated.date() if last_calculated else None

                if user_start is None:
                    calc_end = yesterday
                    if last_date:
                        calc_start = last_date + timedelta(days=1)
                    else:
                        # Pass created_date to optimize search
                        calc_start = get_earliest_log_date(
                            cassandra_session, 
                            thingid,
                            created_date=created_date,
                            scan_end=calc_end
                        )
                        if not calc_start:
                            logger.warning(f"No logs found for {thingid}")
                            continue
                else:
                    calc_end = user_end or user_start
                    if last_date:
                        proposed_start = last_date + timedelta(days=1)
                        calc_start = max(proposed_start, user_start) if proposed_start < user_start else user_start
                    else:
                        # Use created_date if available and within range
                        if created_date and created_date <= user_start:
                            calc_start = created_date
                            logger.info(f"Using createdOn date {created_date} for {thingid}")
                        else:
                            calc_start = get_earliest_log_date(
                                cassandra_session,
                                thingid,
                                created_date=created_date,
                                scan_end=calc_end
                            )
                            if not calc_start:
                                logger.warning(f"No logs found for {thingid}")
                                continue
                            calc_start = min(calc_start, user_start)

            if calc_start > calc_end:
                logger.info(f"Nothing to calculate for {thingid} in given range.")
                continue

            logger.info(f"Calculating run hours for {thingid} from {calc_start} to {calc_end}")
            process_asset_for_date(
                thingid, 
                cassandra_session, 
                pg_conn, 
                calc_start, 
                calc_end, 
                force_update
            )

    except Exception as e:
        logger.error(f"Critical error in main execution: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        pg_conn.close()
        cassandra_session.shutdown()
        logger.info(f"Processing complete. Total time: {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    main()