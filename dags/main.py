from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json,
)
from datawarehouse.dwh import staging_table, core_table

# Define the local timezone
local_tz = pendulum.timezone("Europe/London")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"

# DAG 1: produce_json
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag_produce:

    # Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json = save_to_json(extract_data)

    # Define dependancies
    playlist_id >> video_ids >> extract_data >> save_to_json

# DAG 2: update_db
with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into staging and core schemas',
    schedule='0 15 * * *',
    catchup=False,
    # schedule=None,
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependancies
    update_staging >> update_core 
