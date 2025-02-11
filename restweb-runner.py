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

restweb_main = MySQL(mysql_host=config.mysql_host, 
                     mysql_user = config.mysql_user, 
                     mysql_database = config.mysql_restweb_db,
                     mysql_password = config.mysql_password)

restweb_searchjobs = MySQL(mysql_host = config.mysql_host, 
                     mysql_user = config.mysql_user, 
                     mysql_database = config.mysql_restweb_searchjobs_db,
                     mysql_password = config.mysql_password)

restweb_matching_ids = MySQL(mysql_host = config.mysql_host, 
                     mysql_user = config.mysql_user, 
                     mysql_database = config.mysql_restweb_matching_ids_db,
                     mysql_password = config.mysql_password)

load_dotenv(find_dotenv())
SECRET_KEY = os.environ['SECRET_KEY']

def generate_token(data, expiration=3600):
    """Generate a token with an expiration time."""
    serializer = URLSafeTimedSerializer(SECRET_KEY)
    return serializer.dumps(data)

# Worker function to process jobs from the queue
def worker(entry):

    job_start_time = datetime.now()

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
    is_active = entry[12]
    last_run = entry[13]

    if filter_include:
        filter_include = entry[10].split(",")
    else:
        filter_include = None

    if filter_exclude:
        filter_exclude = entry[11].split(",")
    else:
        filter_exclude = None


    tablename = f"{job_id}_{zipcode}_{radius}"

    Kleinanzeigen.runner(MySQL_DB = restweb_searchjobs,
                        postalcode = zipcode, 
                        radius = radius, 
                        tablename = tablename)
    
    df_entry = restweb_searchjobs.get_dataframe(table=tablename, column="*", add_query=f"WHERE timestamp > '{job_start_time}'")
    # df_entry = restweb_searchjobs.get_dataframe(table=tablename, column="*", add_query=f"WHERE timestamp < '{job_start_time}'")

    df_entry_filtered = filter_dataframe(df_entry, price_min, price_max, rooms_min, rooms_max, sqm_min, sqm_max, filter_include, filter_exclude)
    restweb_matching_ids.create_table(table=tablename, columns=config.mysql_columns_matching_ids, types=config.mysql_types_matching_ids)
    if len(df_entry_filtered) > 0:
        restweb_matching_ids.write_list(tablename, config.mysql_columns_matching_ids[:-1], [[int(x), job_start_time] for x in df_entry_filtered.id_index.values])
        # Call api of restweb to notify about new results
        data = job_id
        token = generate_token(data)
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'}   
        payload = {'indicies' : df_entry_filtered.id_index.values.tolist(),
                    'tablename' : tablename}

        # Send the request
        response = requests.post(config.api_url, json=payload, headers=headers)

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


# Outer loop to check for new entries and assign jobs
def outer_loop():
    while True:
        print("# Checking for jobs to run")
                
        new_entries = restweb_main.execute(query="SELECT * FROM search_jobs WHERE is_active = 1  AND last_run < NOW() - INTERVAL 5 MINUTE", fetch=True)
        print(new_entries)
        for entry in new_entries:
            print(f'## START: {entry}')
            worker(entry)
            job_id = entry[0]
            restweb_main.execute(f"UPDATE search_jobs SET last_run = '{datetime.now()}' WHERE id = {job_id};")

        # Wait for 10 seconds before checking again
        time.sleep(30)

if __name__ == "__main__":
    # Start worker processes
    outer_loop()