

from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, time, timedelta, timezone, date
from cassandra.cluster import Cluster
from config.settings import settings
import logging
from cassandra.query import SimpleStatement
logger = logging.getLogger(__name__)

uae_tz = timezone(timedelta(hours=4))
def utc_to_uae(dt_utc):
    return dt_utc.replace(tzinfo=timezone.utc).astimezone(uae_tz)

def connect_to_cassandra():
    try:
        cluster = Cluster(
            [settings.CASSANDRA_HOST],
            load_balancing_policy=DCAwareRoundRobinPolicy(settings.CASSANDRA_LOCAL_DC or "datacenter1"),
            protocol_version=settings.CASSANDRA_PROTOCOL_VERSION
        )
        session = cluster.connect(settings.CASSANDRA_KEYSPACE)
        logger.info(f"✅ Connected to Cassandra at {settings.CASSANDRA_HOST}")
        return session
    except Exception as e:
        logger.error(f"❌ Error connecting to Cassandra: {e}")
        raise
def fetch_logs_for_day(session, thingid, datadate_utc_date):
    datadate_utc = datetime.combine(datadate_utc_date, time.min).replace(tzinfo=timezone.utc)
    query = """
        SELECT datatime, data 
        FROM big_data_store.run_status
        WHERE thingid = %s AND datadate = %s
        LIMIT 1000
    """
    try:
        rows = session.execute(query, (thingid, datadate_utc))
        results = []
        for row in rows:
            dt = row.datatime
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            results.append((dt, row.data.strip()))
        results.sort(key=lambda x: x[0])
        logger.info(f"Fetched {len(results)} logs for {thingid} on {datadate_utc_date}")
        return results
    except Exception as e:
        logger.error(f"Error fetching logs for {thingid} on {datadate_utc_date}: {e}")
        return []
    

def get_earliest_log_date(session, thingid, created_date=None, max_days_back=365, scan_end=None):
    """
    Find the earliest log date for an asset, using created_date if available to optimize search
    
    Args:
        session: Cassandra session
        thingid: Asset identifier
        created_date: Optional date when asset was created (to optimize search)
        max_days_back: Maximum days to look back (default: 365)
        scan_end: End date for scanning (default: yesterday)
        
    Returns:
        date: Earliest log date found or None
    """
    if scan_end is None:
        scan_end = date.today() - timedelta(days=1)

    # Adjust search range based on created_date if available
    if created_date:
        # Don't search before the asset was created
        effective_max_days_back = min(max_days_back, (scan_end - created_date).days)
        scan_start = max(created_date, scan_end - timedelta(days=max_days_back))
        logger.debug(f"Using created_date {created_date} to optimize search for {thingid}")
    else:
        effective_max_days_back = max_days_back
        scan_start = scan_end - timedelta(days=max_days_back)

    current_date = scan_start
    found_date = None

    while current_date <= scan_end:
        try:
            # Convert date to timestamp at midnight UTC
            datadate_ts = datetime.combine(current_date, time.min).replace(tzinfo=timezone.utc)
            
            query = """
                SELECT datatime FROM big_data_store.run_status
                WHERE thingid = %s AND datadate = %s
                LIMIT 1
            """
            result = session.execute(query, (thingid, datadate_ts))
            
            if result.one():
                logger.debug(f"Found log for {thingid} on {current_date}")
                found_date = current_date
                # No need to check further dates - return the first found
                logger.info(f"Earliest log found for {thingid} on {found_date}")
                return found_date
                
        except Exception as e:
            logger.error(f"Query error on {current_date} for {thingid}: {e}")

        current_date += timedelta(days=1)

    if found_date is None:
        logger.warning(f"No logs found for {thingid} between {scan_start} and {scan_end}")
    return found_date