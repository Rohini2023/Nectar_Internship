



from datetime import datetime, timedelta, time, timezone
import logging
from collections import defaultdict
from app.cassandra_ops import fetch_logs_for_day
from psycopg2.extras import execute_batch
from app.postgres_ops import insert_or_update_run_hours_batch, run_hour_exists
logger = logging.getLogger(__name__)

uae_tz = timezone(timedelta(hours=4))
SECONDS_IN_DAY = 86400

def utc_to_uae(dt_utc):
    return dt_utc.replace(tzinfo=timezone.utc).astimezone(uae_tz)

def _process_duration(start_dt, end_dt, daily_on_seconds):
    """Distribute duration across calendar days"""
    duration = (end_dt - start_dt).total_seconds()
    if duration <= 0:
        return

    remaining = duration
    current_time = start_dt
    
    while remaining > 0:
        day = current_time.date()
        day_end = datetime.combine(day + timedelta(days=1), time.min).replace(tzinfo=uae_tz)
        chunk = min((day_end - current_time).total_seconds(), remaining)
        daily_on_seconds[day] += chunk
        remaining -= chunk
        current_time = day_end
        logger.debug(f"Added {chunk} ON seconds to {day}")

def _force_update_hours(pg_conn, thingid, start_date, end_date, records, force_update):
    """Handle the complete force update operation"""
    try:
        with pg_conn.cursor() as cur:
            # Set timezone explicitly
            cur.execute("SET TIME ZONE 'Asia/Dubai';")

            if force_update:
                # Delete by date range to ensure we catch all variants
                delete_query = """
                    DELETE FROM run_hours 
                    WHERE thingid = %s 
                    AND datadate >= %s 
                    AND datadate < %s
                """
                cur.execute(delete_query, (
                    thingid,
                    datetime.combine(start_date, time.min).replace(tzinfo=uae_tz),
                    datetime.combine(end_date, time.min).replace(tzinfo=uae_tz) + timedelta(days=1)
                ))
                logger.info(f"Force deleted {cur.rowcount} existing records")

            # Insert new records with conflict handling
            execute_batch(cur, """
    INSERT INTO run_hours (thingid, datadate, on_hours, off_hours)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (thingid, datadate) 
    DO UPDATE SET 
        on_hours = EXCLUDED.on_hours,
        off_hours = EXCLUDED.off_hours  -- Proper SQL comment syntax
""", [
    (r["thingid"], r["datadate"], r["on_hours"], r["off_hours"])
    for r in records
])
            
            pg_conn.commit()
            logger.info(f"Successfully upserted {len(records)} records")

    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Database operation failed: {str(e)}")
        raise

def process_asset_for_date(thingid, cassandra_session, pg_conn, start_date, end_date, force_update=False):
    try:
        logger.info(f"Processing {thingid} from {start_date} to {end_date} (force_update={force_update})")
        
        # Convert dates to UAE timezone at midnight
        uae_start = datetime.combine(start_date, time.min).replace(tzinfo=uae_tz)
        uae_end = datetime.combine(end_date, time.min).replace(tzinfo=uae_tz) + timedelta(days=1)
        
        # Initialize tracking variables
        daily_on_seconds = defaultdict(int)
        days_with_logs = set()
        current_on_start = None
        max_on_duration = timedelta(hours=24)  # Safety limit

        # Process each day in range
        current_date = start_date
        while current_date <= end_date:
            cassandra_date = datetime.combine(current_date, time.min).replace(tzinfo=uae_tz).astimezone(timezone.utc).date()
            logs = fetch_logs_for_day(cassandra_session, thingid, cassandra_date)
            logger.info(f"Fetched {len(logs)} logs for {thingid} on {current_date}")

            if logs:
                days_with_logs.add(current_date)
                for dt_utc, state in logs:
                    dt_uae = utc_to_uae(dt_utc)
                    state = state.upper().strip()

                    if state == "ON":
                        if current_on_start is None:
                            current_on_start = dt_uae
                            logger.debug(f"ON state started at {dt_uae}")
                    elif state == "OFF" and current_on_start is not None:
                        _process_duration(current_on_start, dt_uae, daily_on_seconds)
                        current_on_start = None
                        logger.debug(f"OFF state at {dt_uae}")
            
            current_date += timedelta(days=1)

        # Handle any hanging ON state with safety limit
        if current_on_start is not None:
            end_time = min(uae_end, current_on_start + max_on_duration)
            _process_duration(current_on_start, end_time, daily_on_seconds)
            logger.warning(f"Auto-terminated hanging ON state at {end_time}")

        # Prepare database records
        records_to_upsert = []
        current_date = start_date
        while current_date <= end_date:
            if current_date in days_with_logs:
                on_sec = round(daily_on_seconds.get(current_date, 0))
                off_sec = SECONDS_IN_DAY - on_sec
            else:
                on_sec = 0
                off_sec = SECONDS_IN_DAY
                logger.info(f"No logs for {current_date}, defaulting to 0 ON seconds")

            record_date = datetime.combine(current_date, time.min).replace(tzinfo=uae_tz)
            
            # Add this check before adding to records_to_upsert
            if not force_update and run_hour_exists(pg_conn, thingid, record_date):
                logger.debug(f"Skipping existing record for {current_date}")
                current_date += timedelta(days=1)
                continue

            records_to_upsert.append({
                "thingid": thingid,
                "datadate": record_date,
                "on_hours": on_sec,
                "off_hours": off_sec
            })
            current_date += timedelta(days=1)

        # Execute the force update
        if records_to_upsert:
            _force_update_hours(pg_conn, thingid, start_date, end_date, records_to_upsert, force_update)

    except Exception as e:
        logger.error(f"Error processing {thingid}: {str(e)}", exc_info=True)
        raise