
import psycopg2
import logging
from psycopg2.extras import execute_batch
from config.settings import settings
from datetime import datetime, time, timedelta
from pytz import timezone
logger = logging.getLogger(__name__)

# UAE timezone object
uae_tz = timezone("Asia/Dubai")

def to_uae_midnight(date_input):
    """Convert date/datetime to UAE midnight (00:00:00+04:00)."""
    if isinstance(date_input, datetime):
        if date_input.tzinfo:
            date_input = date_input.astimezone(uae_tz).date()
        else:
            date_input = date_input.date()
    return uae_tz.localize(datetime.combine(date_input, time.min))

def connect_postgres():
    try:
        conn = psycopg2.connect(
            host=settings.POSTGRES_HOST,
            database=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD
        )
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'Asia/Dubai';")
        return conn
    except Exception as e:
        logger.error(f"❌ Error connecting to PostgreSQL: {e}")
        raise

def get_last_calculated_date(conn, thingid):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(datadate) 
                FROM run_hours 
                WHERE thingid = %s
            """, (thingid,))
            result = cur.fetchone()
            if result and result[0]:
                return result[0].astimezone(uae_tz)
            return None
    except Exception as e:
        logger.error(f"Error fetching last calculated date for {thingid}: {e}")
        return None

def run_hour_exists(conn, thingid, datadate):
    try:
        if datadate.tzinfo is None:
            datadate = uae_tz.localize(datadate)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM run_hours 
                WHERE thingid = %s 
                AND datadate AT TIME ZONE 'Asia/Dubai' = %s AT TIME ZONE 'Asia/Dubai'
            """, (thingid, datadate))
            return cur.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking existence: {e}")
        return False

def insert_or_update_run_hours_batch(conn, records, force_update=False):
    try:
        if not records:
            return 0

        thingid = records[0]["thingid"]
        # Extract just the date part for deletion (to avoid timezone issues)
        date_values = [r["datadate"].date() for r in records]

        with conn.cursor() as cur:
            # Ensure we're in UAE timezone
            cur.execute("SET TIME ZONE 'Asia/Dubai';")

            if force_update:
                # Delete by date range to catch all timezone variants
                min_date = min(date_values)
                max_date = max(date_values)
                cur.execute(
                    """DELETE FROM run_hours 
                    WHERE thingid = %s 
                    AND datadate >= %s 
                    AND datadate < %s + interval '1 day'""",
                    (thingid, min_date, max_date)
                )
                logger.info(f"🗑️ Deleted {cur.rowcount} existing records for force update")

            # Insert new records with explicit timezone
            execute_batch(cur, """
                INSERT INTO run_hours (thingid, datadate, on_hours, off_hours)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (thingid, datadate) DO UPDATE
                SET on_hours = EXCLUDED.on_hours,
                    off_hours = EXCLUDED.off_hours
            """, [
                (r["thingid"], r["datadate"], r["on_hours"], r["off_hours"])
                for r in records
            ])

        conn.commit()
        count = len(records)
        logger.info(f"✅ Processed {count} run hour records")
        return count

    except Exception as e:
        logger.error(f"❌ Error in batch operation: {e}")
        conn.rollback()
        raise