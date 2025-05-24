from app.cassandra_ops import connect_to_cassandra, get_earliest_log_date
from app.postgres_ops import connect_postgres, get_last_calculated_date
from app.run_hour_calculation import process_asset_for_date
from datetime import date, datetime, timedelta
import sys
import logging
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
        sys.exit(1)

def get_date_range_from_args():
    parser = argparse.ArgumentParser(description='Process run hours for assets')
    parser.add_argument('dates', nargs='*', help='Date range to process (start_date end_date)')
    parser.add_argument('--force', action='store_true', help='Force reprocessing of all dates in range')
    args = parser.parse_args()

    yesterday = date.today() - timedelta(days=1)
    force_update = args.force

    if len(args.dates) == 0:
        return None, None, force_update  # fallback on last_calculated + 1 → yesterday
    elif len(args.dates) == 1:
        d = parse_date(args.dates[0])
        return d, d, force_update
    elif len(args.dates) == 2:
        start = parse_date(args.dates[0])
        end = parse_date(args.dates[1])
        if end < start:
            logger.error("End date cannot be earlier than start date.")
            sys.exit(1)
        return start, end, force_update
    else:
        logger.error("Invalid arguments. Usage: python main.py [start_date] [end_date] [--force]")
        sys.exit(1)

def main():
    cassandra_session = connect_to_cassandra()
    pg_conn = connect_postgres()

    user_start, user_end, force_update = get_date_range_from_args()
    yesterday = date.today() - timedelta(days=1)

    # Default fallback asset list
    asset_ids = ["AC_001"] 
    
    for thingid in asset_ids:
        logger.info(f"Processing {thingid} (force={force_update})")

        if force_update:
            # In force mode, we completely ignore last calculated date
            if user_start is None:
                # If no dates specified, use yesterday only
                calc_start = calc_end = yesterday
            else:
                calc_start = user_start
                calc_end = user_end or user_start
        else:
            # Normal processing logic
            last_calculated = get_last_calculated_date(pg_conn, thingid)
            last_date = last_calculated.date() if last_calculated else None

            if user_start is None:
                # Case: no input → run from last + 1 to yesterday
                calc_end = yesterday
                if last_date:
                    calc_start = last_date + timedelta(days=1)
                else:
                    earliest_log = get_earliest_log_date(cassandra_session, thingid)
                    if not earliest_log:
                        logger.warning(f"No logs found in Cassandra for {thingid}.")
                        continue
                    calc_start = earliest_log
            else:
                calc_end = user_end or user_start
                if last_date:
                    proposed_start = last_date + timedelta(days=1)
                    # Fill gap from last_date+1 up to user_start if gap exists
                    if proposed_start < user_start:
                        calc_start = proposed_start
                    else:
                        calc_start = user_start
                else:
                    # No last_date, so start from earliest log or user_start (whichever is earlier)
                    earliest_log = get_earliest_log_date(cassandra_session, thingid,
                                                        max_days_back=365,
                                                        scan_end=calc_end)
                    if not earliest_log:
                        logger.warning(f"No logs found in Cassandra for {thingid}.")
                        continue
                    calc_start = min(earliest_log, user_start)

        if calc_start > calc_end:
            logger.info(f"Nothing to calculate for {thingid} in given range.")
            continue

        logger.info(f"Calculating run hours for {thingid} from {calc_start} to {calc_end}")
        process_asset_for_date(thingid, cassandra_session, pg_conn, calc_start, calc_end, force_update)

    pg_conn.close()
    cassandra_session.shutdown()
    logger.info("Run hour processing complete.")

if __name__ == "__main__":
    main()