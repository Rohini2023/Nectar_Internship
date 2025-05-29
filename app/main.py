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
        list: List of asset IDs (thingIds) or fallback ["AC_001"] on failure
    """
    start_time = time.time()
    asset_result = fetch_and_filter_assets()
    
    # Handle API failure cases
    if not asset_result['success']:
        logger.error(f"Asset fetch failed: {asset_result['error']}")
        if asset_result.get('retryable', False):
            logger.warning("Retryable error - using fallback asset AC_001")
        else:
            logger.error("Non-retryable error - using fallback asset AC_001")
        return ["AC_001"]  # Fallback asset
    
    # Log successful fetch metrics
    logger.info(f"Fetched {asset_result['data']['filtered_count']} assets in {time.time() - start_time:.2f}s")
    return [asset['thingId'] for asset in asset_result['data']['assets']]

def main():
    """Main execution flow for run hour calculation"""
    start_time = time.time()
    
    # Initialize database connections
    cassandra_session = connect_to_cassandra()  # For status logs
    pg_conn = connect_postgres()  # For calculated results
    
    try:
        # 1. Parse command line arguments
        user_start, user_end, force_update = get_date_range_from_args()
        yesterday = date.today() - timedelta(days=1)

        # 2. Fetch assets to process
        asset_ids = handle_asset_fetching()
        logger.info(f"Processing {len(asset_ids)} assets: {asset_ids}")

        # 3. Process each asset
        for thingid in asset_ids:
            logger.info(f"Processing {thingid} (force={force_update})")

            # Determine calculation range based on mode
            if force_update:
                # Force mode - use explicit dates or yesterday
                if user_start is None:
                    calc_start = calc_end = yesterday
                else:
                    calc_start = user_start
                    calc_end = user_end or user_start
            else:
                # Normal mode - calculate from last processed date
                last_calculated = get_last_calculated_date(pg_conn, thingid)
                last_date = last_calculated.date() if last_calculated else None

                if user_start is None:
                    # Automatic range calculation
                    calc_end = yesterday
                    if last_date:
                        calc_start = last_date + timedelta(days=1)
                    else:
                        # First-time processing - find earliest log
                        earliest_log = get_earliest_log_date(cassandra_session, thingid)
                        if not earliest_log:
                            logger.warning(f"No logs found in Cassandra for {thingid}.")
                            continue
                        calc_start = earliest_log
                else:
                    # User-specified range with gap filling
                    calc_end = user_end or user_start
                    if last_date:
                        proposed_start = last_date + timedelta(days=1)
                        calc_start = proposed_start if proposed_start < user_start else user_start
                    else:
                        earliest_log = get_earliest_log_date(cassandra_session, thingid,
                                                            max_days_back=365,
                                                            scan_end=calc_end)
                        if not earliest_log:
                            logger.warning(f"No logs found in Cassandra for {thingid}.")
                            continue
                        calc_start = min(earliest_log, user_start)

            # Skip if no date range to process
            if calc_start > calc_end:
                logger.info(f"Nothing to calculate for {thingid} in given range.")
                continue

            # 4. Execute run hour calculation
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
        # 5. Cleanup resources
        pg_conn.close()
        cassandra_session.shutdown()
        logger.info(f"Run hour processing complete. Total execution time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()