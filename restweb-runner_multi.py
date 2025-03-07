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
from multiprocessing import Process, Queue

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

restweb_searchjobs = MySQL(
    mysql_host=config.mysql_host,
    mysql_user=config.mysql_user,
    mysql_database=config.mysql_restweb_searchjobs_db,
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

def worker(entry, result_queue):
    """Process a job from the queue."""
    try:
        job_start_time = datetime.now()

        # Extract job details from the entry
        job_id, user_id, zipcode, radius, price_min, price_max, rooms_min, rooms_max, sqm_min, sqm_max, filter_include, filter_exclude, is_active, last_run = entry

        # Process filter_include and filter_exclude
        filter_include = filter_include.split(",") if filter_include else None
        filter_exclude = filter_exclude.split(",") if filter_exclude else None

        # Define table name based on job details
        tablename = f"{job_id}_{zipcode}_{radius}"

        # Run Kleinanzeigen search
        Kleinanzeigen.runner(
            MySQL_DB=restweb_searchjobs,
            postalcode=zipcode,
            radius=radius,
            tablename=tablename
        )

        # Fetch new entries from the database
        df_entry = restweb_searchjobs.get_dataframe(
            table=tablename,
            column="*",
            add_query=f"WHERE timestamp > '{job_start_time}'"
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

        # Create a table for matching IDs if it doesn't exist
        restweb_matching_ids.create_table(
            table=tablename,
            columns=config.mysql_columns_matching_ids,
            types=config.mysql_types_matching_ids
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
            response = requests.post(config.api_url, json=payload, headers=headers)

        # Put the result in the queue
        result_queue.put((job_id, True))  # (job_id, success)
    except Exception as e:
        result_queue.put((job_id, False, str(e)))  # (job_id, success, error_message)

def outer_loop():
    """Continuously check for new jobs and process them using multiprocessing."""
    while True:
        print("# Checking for jobs to run")

        # Fetch new jobs that are active and haven't been run in the last 5 minutes
        new_entries = restweb_main.execute(
            query="SELECT * FROM search_jobs WHERE is_active = 1 AND last_run < NOW() - INTERVAL 5 MINUTE",
            fetch=True
        )
        print(new_entries)

        # Use a Queue to collect results from worker processes
        result_queue = Queue()

        # Create a list to hold active processes
        processes = []

        # Process each new job using multiprocessing
        for entry in new_entries:
            print(f'## START: {entry}')
            job_id = entry[0]

            # Create a new process for the worker
            process = Process(target=worker, args=(entry, result_queue))
            process.start()
            processes.append(process)

            # Limit the number of concurrent processes to 4
            if len(processes) >= 4:
                # Wait for the first process to finish
                processes[0].join()
                processes.pop(0)

        # Wait for all remaining processes to finish
        for process in processes:
            process.join()

        # Collect results from the queue
        while not result_queue.empty():
            result = result_queue.get()
            job_id, success, *error_message = result
            if success:
                print(f"Job {job_id} completed successfully.")
                # Update the last_run timestamp for the job
                restweb_main.execute(
                    f"UPDATE search_jobs SET last_run = '{datetime.now()}' WHERE id = {job_id};"
                )
            else:
                print(f"Job {job_id} failed with error: {error_message[0]}")

        # Wait for 30 seconds before checking again
        time.sleep(30)

if __name__ == "__main__":
    # Start the outer loop to continuously check for and process jobs
    outer_loop()