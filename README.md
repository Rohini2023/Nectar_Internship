# AUTOMATED RUN HOUR CALCULATION AND STORAGE SYSTEM

This project automates the calculation of run hours for assets based on ON/OFF status logs from Cassandra and stores the results in a PostgreSQL database. It includes token-based authentication, supports paginated API asset retrieval, handles timezone conversion, and runs as a daily scheduled task.
# Features

- Token-based asset API authentication
- Cassandra queries without ALLOW FILTERING
- Pagination handling for large asset lists
- Batch insert into PostgreSQL
- OS-level scheduling 
- allow users to specify either a single date, a range of dates, or use yesterday's date by default for run hour calculation
# Project Structure

Project/ 
|----app/
| |---__init__.py
| |---main.py # Orchestrates full workflow
| |---auth.py #Token-based API authentication
| |---asset_api.py #Handles paginated asset fetching
| |---cassandra_ops.py #Cassandra operations
| |---logger.py #Centralized logging
| |---postgres_ops.py #PostgreSQL operations
| |---run_hour_calculation.py #Core logic for run_hourcalculation
| |---utils.py #Utility functions
|----config/
| |---__init__.py
| |---env_loader.py 
| |---settings.py
|---.env.development
|---.env.production
|-----DB
| |---casandra_insert.py
|----requirements.txt #Dependencies
|----README.md #Project documentation

Features:

- Automatically calculates run hours based on ON/OFF status transitions.
- Fetches asset data from a paginated API.
- Inserts or updates records in PostgreSQL (idempotent operation).
- Converts timestamps to UAE timezone.
- Daily scheduling using OS scheduler
- Avoids `ALLOW FILTERING` in Cassandra queries.

Install dependencies
- `pip install -r requirements.txt`

configuration
- `python -m app.config.base`
usage 
- `python -m app.main`

Database Notes
- Cassandra database :`big_data_store`
- Cassandra Table
    - Primary key : run_status(thingid, datadate)
    - Columns: thingid, datadate, status, run_status
    - Avoids ALLOW FILTERING by using a composite primary key
    - `run_status` (ON/OFF status logs)
- PostgreSQL database: `big_data_store`
- PostgreSQL Table
    - Columns:thingid,datadate,on_hours,off_hours
    - `run_hours` (calculated run hours)
- Author
- License 