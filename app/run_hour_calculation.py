





from datetime import datetime, timedelta, time, timezone
import logging
from collections import defaultdict
from app.cassandra_ops import fetch_logs_for_day
from psycopg2.extras import execute_batch
from app.postgres_ops import insert_or_update_run_hours_batch, run_hour_exists

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Enable debug logging

uae_tz = timezone(timedelta(hours=4))
MILLISECONDS_IN_DAY = 86400000  # 24*60*60*1000

def utc_to_uae(dt_utc):
    """Convert UTC datetime to UAE timezone with validation"""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    elif dt_utc.tzinfo != timezone.utc:
        logger.warning(f"Non-UTC timestamp received: {dt_utc}")
    return dt_utc.astimezone(uae_tz)

def _process_duration(start_dt, end_dt, daily_on_milliseconds):
    """Distribute duration across calendar days with debug logging"""
    logger.debug(f"Processing duration from {start_dt} to {end_dt}")
    
    duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
    logger.debug(f"Total duration: {duration_ms}ms")
    
    if duration_ms <= 0:
        logger.debug("Zero or negative duration skipped")
        return

    remaining_ms = duration_ms
    current_time = start_dt
    
    while remaining_ms > 0:
        day = current_time.date()
        day_end = datetime.combine(day + timedelta(days=1), time.min).replace(tzinfo=uae_tz)
        chunk_ms = min(int((day_end - current_time).total_seconds() * 1000), remaining_ms)
        
        logger.debug(f"Adding {chunk_ms}ms to {day} (current_time: {current_time}, day_end: {day_end})")
        
        daily_on_milliseconds[day] += chunk_ms
        remaining_ms -= chunk_ms
        current_time = day_end
        
        if remaining_ms > 0:
            logger.debug(f"Continuing with {remaining_ms}ms remaining")

def _force_update_hours(pg_conn, thingid, start_date, end_date, records, force_update):
    """Enhanced force update with validation"""
    try:
        with pg_conn.cursor() as cur:
            # Set timezone explicitly
            cur.execute("SET TIME ZONE 'Asia/Dubai';")

            if force_update:
                # Verify existing records before deletion
                cur.execute("""
                    SELECT COUNT(*) FROM run_hours 
                    WHERE thingid = %s AND datadate >= %s AND datadate < %s
                """, (
                    thingid,
                    datetime.combine(start_date, time.min).replace(tzinfo=uae_tz),
                    datetime.combine(end_date, time.min).replace(tzinfo=uae_tz) + timedelta(days=1)
                ))
                existing_count = cur.fetchone()[0]
                logger.info(f"Found {existing_count} existing records to replace")

                # Delete by date range
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
                logger.info(f"Deleted {cur.rowcount} existing records")

            # Insert new records with conflict handling
            execute_batch(cur, """
                INSERT INTO run_hours (thingid, datadate, on_hours, off_hours)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (thingid, datadate) 
                DO UPDATE SET 
                    on_hours = EXCLUDED.on_hours,
                    off_hours = EXCLUDED.off_hours
            """, [
                (r["thingid"], r["datadate"], r["on_hours"], r["off_hours"])
                for r in records
            ])
            
            pg_conn.commit()
            logger.info(f"Upserted {len(records)} records")

    except Exception as e:
        pg_conn.rollback()
        logger.error(f"Database operation failed: {str(e)}", exc_info=True)
        raise

def process_asset_for_date(thingid, cassandra_session, pg_conn, start_date, end_date, force_update=False):
    """Enhanced processing with validation checks"""
    try:
        logger.info(f"Processing {thingid} from {start_date} to {end_date} (force_update={force_update})")
        
        # Convert dates to UAE timezone at midnight
        uae_start = datetime.combine(start_date, time.min).replace(tzinfo=uae_tz)
        uae_end = datetime.combine(end_date, time.min).replace(tzinfo=uae_tz) + timedelta(days=1)
        
        # Initialize tracking
        daily_on_milliseconds = defaultdict(int)
        days_with_logs = set()
        current_on_start = None
        max_on_duration = timedelta(hours=24)
        total_logs_processed = 0

        # Process each day in range
        current_date = start_date
        while current_date <= end_date:
            cassandra_date = datetime.combine(current_date, time.min).replace(tzinfo=uae_tz).astimezone(timezone.utc).date()
            logs = fetch_logs_for_day(cassandra_session, thingid, cassandra_date)
            logger.info(f"Fetched {len(logs)} logs for {thingid} on {current_date}")

            if logs:
                days_with_logs.add(current_date)
                previous_state = None
                
                for dt_utc, state in logs:
                    state = state.upper().strip()
                    dt_uae = utc_to_uae(dt_utc)
                    total_logs_processed += 1
                    
                    # Debug boundary checks
                    if dt_uae.date() != current_date:
                        logger.warning(
                            f"Log crosses date boundary: {dt_utc} UTC -> {dt_uae} UAE "
                            f"(expected date: {current_date})"
                        )

                    logger.debug(f"Processing log #{total_logs_processed}: {dt_uae} | State: {state}")
                    
                    if state == "ON":
                        if current_on_start is None:
                            current_on_start = dt_uae
                            logger.debug(f"ON state started at {dt_uae}")
                        elif previous_state == "ON":
                            logger.warning(f"Consecutive ON states at {dt_uae}")
                    elif state == "OFF":
                        if current_on_start is not None:
                            _process_duration(current_on_start, dt_uae, daily_on_milliseconds)
                            current_on_start = None
                            logger.debug(f"OFF state at {dt_uae}")
                        elif previous_state == "OFF":
                            logger.warning(f"Consecutive OFF states at {dt_uae}")
                    
                    previous_state = state
            
            current_date += timedelta(days=1)

        # Handle any hanging ON state
        if current_on_start is not None:
            end_time = min(uae_end, current_on_start + max_on_duration)
            logger.warning(f"Auto-terminating hanging ON state started at {current_on_start}")
            _process_duration(current_on_start, end_time, daily_on_milliseconds)

        # Validation: Check total calculated time
        total_calculated_ms = sum(daily_on_milliseconds.values())
        expected_ms = (uae_end - uae_start).total_seconds() * 1000 * (end_date - start_date).days
        time_discrepancy = abs(total_calculated_ms - expected_ms)
        
        if time_discrepancy > 1000:  # 1 second tolerance
            logger.error(
                f"Time calculation mismatch! Calculated: {total_calculated_ms}ms, "
                f"Expected: ~{expected_ms}ms, Difference: {time_discrepancy}ms"
            )

        # Prepare database records
        records_to_upsert = []
        current_date = start_date
        while current_date <= end_date:
            record_date = datetime.combine(current_date, time.min).replace(tzinfo=uae_tz)
            
            if current_date in days_with_logs:
                on_ms = daily_on_milliseconds.get(current_date, 0)
                off_ms = MILLISECONDS_IN_DAY - on_ms
                logger.debug(f"Date {current_date}: ON={on_ms}ms, OFF={off_ms}ms")
            else:
                on_ms = 0
                off_ms = MILLISECONDS_IN_DAY
                logger.info(f"No logs for {current_date}, defaulting to 0ms ON time")

            if not force_update and run_hour_exists(pg_conn, thingid, record_date):
                logger.debug(f"Skipping existing record for {current_date}")
                current_date += timedelta(days=1)
                continue

            records_to_upsert.append({
                "thingid": thingid,
                "datadate": record_date,
                "on_hours": on_ms,
                "off_hours": off_ms
            })
            current_date += timedelta(days=1)

        # Execute the update
        if records_to_upsert:
            _force_update_hours(pg_conn, thingid, start_date, end_date, records_to_upsert, force_update)
        else:
            logger.info("No records to update")

        logger.info(
            f"Processing complete for {thingid}. "
            f"Total logs processed: {total_logs_processed}, "
            f"Days with logs: {len(days_with_logs)}"
        )

    except Exception as e:
        logger.error(f"Error processing {thingid}: {str(e)}", exc_info=True)
        raise









