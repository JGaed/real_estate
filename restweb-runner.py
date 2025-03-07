import time
import pandas as pd
import requests
from itsdangerous import URLSafeTimedSerializer
from mysql_wrapper import MySQL
from kleinanzeigen import Kleinanzeigen
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os
import config

# Load environment variables from .env file
load_dotenv(find_dotenv())
SECRET_KEY = os.environ['SECRET_KEY']

# Initialize MySQL connections for different databases
restweb_main = MySQL(
    mysql_host=config.mysql_host,
    mysql_user=config.mysql_user,
    mysql_database=config.mysql_restweb_db,
    mysql_password=config.mysql_password
)

restweb_matching_ids = MySQL(
    mysql_host=config.mysql_host,
    mysql_user=config.mysql_user,
    mysql_database=config.mysql_restweb_matching_ids_db,
    mysql_password=config.mysql_password
)

def generate_token(data, expiration=3600):
    """Generate a token with an expiration time."""
    serializer = URLSafeTimedSerializer(SECRET_KEY)
    return serializer.dumps(data)

def filter_dataframe(df, price_min=None, price_max=None, rooms_min=None, rooms_max=None, sqm_min=None, sqm_max=None, filter_include=None, filter_exclude=None):
    """
    Filters a pandas DataFrame based on the given criteria.

    Parameters:
    - df: pandas DataFrame to filter.
    - price_min: Minimum price.
    - price_max: Maximum price.
    - rooms_min: Minimum number of rooms.
    - rooms_max: Maximum number of rooms.
    - sqm_min: Minimum size in square meters.
    - sqm_max: Maximum size in square meters.
    - filter_include: List of keywords to include in the title or description.
    - filter_exclude: List of keywords to exclude from the title or description.

    Returns:
    - Filtered pandas DataFrame.
    """
    # Apply price filter
    if price_min is not None:
        df = df[df['price'] >= price_min]
    if price_max is not None:
        df = df[df['price'] <= price_max]

    # Apply rooms filter
    if rooms_min is not None:
        df = df[df['rooms'] >= rooms_min]
    if rooms_max is not None:
        df = df[df['rooms'] <= rooms_max]

    # Apply size (sqm) filter
    if sqm_min is not None:
        df = df[df['size'] >= sqm_min]
    if sqm_max is not None:
        df = df[df['size'] <= sqm_max]

    # Apply include filter
    if filter_include is not None:
        include_mask = df['title'].str.contains('|'.join(filter_include), case=False, na=False) | \
                       df['description'].str.contains('|'.join(filter_include), case=False, na=False)
        df = df[include_mask]

    # Apply exclude filter
    if filter_exclude is not None:
        exclude_mask = ~(df['title'].str.contains('|'.join(filter_exclude), case=False, na=False) | \
                         df['description'].str.contains('|'.join(filter_exclude), case=False, na=False))
        df = df[exclude_mask]

    return df

def worker(entry):
    """Process a job from the queue."""
    job_start_time = datetime.now()

    # Extract job details from the entry
    job_id, user_id, zipcode, radius, price_min, price_max, rooms_min, rooms_max, sqm_min, sqm_max, filter_include, filter_exclude, is_active, last_run = entry

    # Process filter_include and filter_exclude
    filter_include = filter_include.split(",") if filter_include else None
    filter_exclude = filter_exclude.split(",") if filter_exclude else None

    # Define table name based on job details
    tablename = f"job_{job_id}"

    # Create a table for matching IDs if it doesn't exist
    restweb_matching_ids.create_table(
        table=tablename,
        columns=config.mysql_columns_matching_ids,
        types=config.mysql_types_matching_ids
    )

    restweb_main.create_table(
        table=config.mysql_results_table,
        columns=config.mysql_columns,
        types=config.mysql_types
    )

    restweb_main.create_table(
        table=config.mysql_error_table,
        columns=config.mysql_columns_err,
        types=config.mysql_types_err
    )

    job_matching_ids = [x[0] for x in restweb_matching_ids.get_table(tablename, ['id'], 
                                                 sort_by='created_at', 
                                                 max_entries=100, 
                                                 descending=True)]

    print(job_matching_ids)
    job_ids = [x[0] for x in restweb_main.get_table(config.mysql_results_table, ["id"], add_query=f"WHERE id_index in ({','.join(map(str, job_matching_ids))})")]
 
    print(job_ids)
    attempts = 0
    while attempts <= 5:
        attempts += 1
        try:
            new_offers = Kleinanzeigen.get_search_offers(postalcode=zipcode, 
                                radius=radius, 
                                max_number=100,  
                                end_index=job_ids)
            break
        except Exception as e:
            print(f"[REST][RESTWEB-RUNNER] Failed calling get_offer_indicies: {e}")

    # existing_result_ids = restweb_main.get_table("results", ['id'])
    # existing_result_ids = [x[0] for x in existing_result_ids]  

    # Sraping new offers and add to results table
    attempts = 0
    while attempts <= 5:
        attempts += 1
        try:
            Kleinanzeigen.offers_to_mysql(offers = new_offers,
                                          mysql_obj=restweb_main,
                                          mysql_table=config.mysql_results_table,
                                          mysql_table_err=config.mysql_error_table)
            break
        except Exception as e:
            print(f"[REST][RESTWEB-RUNNER] Failed calling get_offer_indicies: {e}")


    new_offers_ids = [x for x in new_offers.offers_indices]
    # Fetch new entries from the database
    df_entry = restweb_main.get_dataframe(
        table='results',
        column="*",
        add_query=f"WHERE timestamp > '{last_run}' AND id in ({','.join(map(str, new_offers_ids))})"
    )


    # Filter the DataFrame based on job criteria
    df_entry_filtered = filter_dataframe(
        df_entry,
        price_min,
        price_max,
        rooms_min,
        rooms_max,
        sqm_min,
        sqm_max,
        filter_include,
        filter_exclude
    )

    # If there are matching results, write them to the database and notify via API
    if len(df_entry_filtered) > 0:
        restweb_matching_ids.write_list(
            tablename,
            config.mysql_columns_matching_ids[:-1],
            [[int(x), job_start_time] for x in df_entry_filtered.id_index.values]
        )

        # Generate a token for API authorization
        token = generate_token(job_id)
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        payload = {
            'indicies': df_entry_filtered.id_index.values.tolist(),
            'tablename': tablename
        }

        # Send the request to the API
        response = requests.post(config.api_url+"/notify", json=payload, headers=headers)
        print(f"[REST][RESWEB-RUNNER] Send {len(df_entry_filtered)} offers to api for Job {job_id}")

def outer_loop():
    """Continuously check for new jobs and process them."""
    while True:
        print("[REST][RESWEB-RUNNER] Checking for jobs to run")

        # Fetch new jobs that are active and haven't been run in the last 5 minutes
        jobs_to_execute = restweb_main.execute(
            query="SELECT * FROM search_jobs WHERE is_active = 1 AND last_run < (CONVERT_TZ(NOW(), @@session.time_zone, 'Europe/Berlin') - INTERVAL 5 MINUTE);",
            fetch=True
        )
        print(jobs_to_execute)

        # Process each new job
        for job in jobs_to_execute:
            print(f'## START: {job}')
            worker(job)
            job_id = job[0]

            # Update the last_run timestamp for the job
            restweb_main.execute(
                f"UPDATE search_jobs SET last_run = '{datetime.now()}' WHERE id = {job_id};"
            )

        # Wait for 30 seconds before checking again
        time.sleep(30)

if __name__ == "__main__":
    # Start the outer loop to continuously check for and process jobs
    outer_loop()