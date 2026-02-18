from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table

local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email':'data@engineers.com',
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(hours=1),
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
}
with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce json data with raw file',
    schedule_interval='0 14 * * *',  # Daily at 2 PM UTC
    catchup=False,
    tags=['youtube', 'video_stats', 'etl'],
) as produce_dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into both staging and core schemas',
    schedule_interval='0 14 * * *',  # Daily at 2 PM UTC
    catchup=False,
    tags=['youtube', 'video_stats', 'etl'],
) as update_dag:

    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core