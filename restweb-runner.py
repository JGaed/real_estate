import time
import pandas as pd
import requests
from itsdangerous import URLSafeTimedSerializer
from mysql_wrapper import MySQL
from multiprocessing import Process, Queue, Lock, Value
from kleinanzeigen import Kleinanzeigen
from datetime import datetime

# Configuration for MySQL connection
from config import mysql_host, mysql_user, mysql_password, mysql_restweb_db, mysql_restweb_searchjobs_db, mysql_restweb_matching_ids_db, mysql_columns_matching_ids, mysql_types_matching_ids

restweb_main = MySQL(mysql_host=mysql_host, 
                     mysql_user = mysql_user, 
                     mysql_database = mysql_restweb_db,
                     mysql_password = mysql_password)

restweb_searchjobs = MySQL(mysql_host=mysql_host, 
                     mysql_user = mysql_user, 
                     mysql_database = mysql_restweb_searchjobs_db,
                     mysql_password = mysql_password)

restweb_matching_ids = MySQL(mysql_host=mysql_host, 
                     mysql_user = mysql_user, 
                     mysql_database = mysql_restweb_matching_ids_db,
                     mysql_password = mysql_password)



# Global variables
MAX_WORKERS = 4
job_queue = Queue()  # Queue to hold jobs
active_workers = Value('i', 0)  # Shared counter for active workers 
lock = Lock()  # Lock for thread-safe updates to active_workers

def generate_token(data, expiration=3600):
    """Generate a token with an expiration time."""
    serializer = URLSafeTimedSerializer(SECRET_KEY)
    return serializer.dumps(data)

# Worker function to process jobs from the queue
def worker(job_queue, active_workers, lock):
    while True:

        job_start_time = datetime.now()

        entry = job_queue.get()  # Get a job from the queue

        if entry is None:  # Sentinel value to stop the worker
            break

        job_id = entry[0]
        user_id = entry[1]
        zipcode = int(entry[2])
        radius = entry[3]
        price_min = entry[4]
        price_max = entry[5]
        rooms_min = entry[6]
        rooms_max = entry[7]
        sqm_min = entry[8]
        sqm_max = entry[9]
        filter_include = entry[10]
        filter_exclude = entry[11]

        if filter_include:
            filter_include = entry[10].split(",")
        else:
            filter_include = None

        if filter_exclude:
            filter_exclude = entry[11].split(",")
        else:
            filter_exclude = None
        is_active = entry[12]
        last_run = entry[13]

        tablename = f"{job_id}_{zipcode}_{radius}"

        Kleinanzeigen.runner(MySQL_DB = restweb_searchjobs,
                            postalcode = zipcode, 
                            radius = radius, 
                            tablename = tablename)
        
        df_entry = restweb_searchjobs.get_dataframe(table=tablename, column="*", add_query=f"WHERE timestamp > '{job_start_time}'")
        df_entry = restweb_searchjobs.get_dataframe(table=tablename, column="*", add_query=f"WHERE timestamp < '{job_start_time}'")

        df_entry_filtered = filter_dataframe(df_entry, price_min, price_max, rooms_min, rooms_max, sqm_min, sqm_max, filter_include, filter_exclude)
        restweb_matching_ids.create_table(table=tablename, columns=mysql_columns_matching_ids, types=mysql_types_matching_ids)
        restweb_matching_ids.write_list(tablename, mysql_columns_matching_ids[:-1], [[int(x), job_start_time] for x in df_entry_filtered.index.values])

        # Call api of restweb to notify about new results
        

        # Chaning last_run in restweb.searchjobs
        restweb_main.execute(f"UPDATE search_jobs SET last_run = '{job_start_time}' WHERE id = {job_id};")

        with lock:
            active_workers.value -= 1  # Decrement the active worker count

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

# Example usage:
# Assuming 'df' is your DataFrame and 'entry' is a list with the filter criteria
# entry = [None, 1000, 2, 4, 50, 100, ['bright', 'sunny'], ['basement']]
# filtered_df = filter_dataframe(df, *entry[4:12])




# Outer loop to check for new entries and assign jobs
def outer_loop(job_queue, active_workers, lock):
    while True:
        with lock:
            if active_workers.value < MAX_WORKERS:
                # Check MySQL for new entries

                
                new_entries = restweb_main.execute(query="SELECT * FROM search_jobs WHERE is_active = 1  AND last_run < NOW() - INTERVAL 5 MINUTE", fetch=True)
                for entry in new_entries:
                    # job_id = entry[0]
                    # user_id = entry[1]
                    # zipcode = entry[2]
                    # radius = entry[3]
                    # price_min = entry[4]
                    # price_max = entry[5]
                    # rooms_min = entry[6]
                    # rooms_max = entry[7]
                    # sqm_min = entry[8]
                    # sqm_max = entry[9]
                    # filter_include = entry[10]
                    # filter_exclude = entry[11]
                    # is_active = entry[12]
                    # last_run = entry[13]
                    job_queue.put((entry))  # Add job to the queue
                    with lock:
                        active_workers.value += 1  # Increment the active worker count
                    print(f"Assigned job {job_id} to a worker")

        # Wait for 10 seconds before checking again
        time.sleep(120)

if __name__ == "__main__":
    # Start worker processes
    worker_processes = []
    for _ in range(MAX_WORKERS):
        p = Process(target=worker, args=(job_queue, active_workers, lock))
        p.start()
        worker_processes.append(p)

    # Start the outer loop in the main process
    try:
        outer_loop(job_queue, active_workers, lock)
    except KeyboardInterrupt:
        print("Shutting down...")

        # Stop the workers by sending sentinel values
        for _ in range(MAX_WORKERS):
            job_queue.put(None)

        # Wait for all worker processes to finish
        for p in worker_processes:
            p.join()

        print("All workers and processes stopped.")