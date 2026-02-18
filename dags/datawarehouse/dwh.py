from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data 
from datawarehouse.data_transformation import transform_data

import logging
from datetime import timedelta, datetime
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = 'yt_api'

@task
def staging_table():
    schema = 'staging'
    conn, cur = None, None
    try:
        # Import modification helpers at runtime to avoid import-time failures
        import sys
        sys.path.append('/opt/airflow/dags')
        from datawarehouse.data_modification import insert_row, update_row, delete_row

        conn, cur = get_conn_cursor()
        YT_data = load_data()
        create_schema(schema)
        create_table(schema)
        table_ids = get_video_ids(cur, schema)

        for row in YT_data:
            # Transform keys to match database columns
            row['Video_Id'] = row.pop('videoId')
            row['Video_Title'] = row.pop('title')
            row['Upload_Date'] = datetime.fromisoformat(row.pop('publishedAt').replace('Z', '+00:00'))
            row['View_Count'] = int(row.pop('viewCount'))
            row['Like_Count'] = int(row.pop('likeCount'))
            row['Comment_Count'] = int(row.pop('commentCount'))
            row['Duration'] = row.pop('duration')
            
            if len(table_ids) == 0 or row['Video_Id'] not in table_ids:
                insert_row(cur, conn, schema, row)
            else:
                if row['Video_Id'] in table_ids:
                    update_row(cur, conn, schema, row)
                else:
                    insert_row(cur, conn, schema, row)
        ids_in_json = {row['Video_Id'] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json
        if ids_to_delete:
            for video_id in ids_to_delete:
                delete_row(cur, conn, schema, video_id)
        logger.info(f"{schema} table update completed")
    except Exception as e:
        print(f"Exception: {e}")
        logger.error(f"Error in staging_table task: {e}")
        logger.info(f"Exception details: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    schema = 'core'
    conn, cur = None, None
    try:
        # Import modification helpers at runtime to avoid import-time failures
        import sys
        sys.path.append('/opt/airflow/dags')
        from datawarehouse.data_modification import insert_row, update_row, delete_row
        conn, cur = get_conn_cursor()
        # Load data from staging
        cur.execute(f"SELECT * FROM staging.{table}")
        staging_data = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        create_schema(schema)
        create_table(schema)
        table_ids = get_video_ids(cur, schema)

        for row_tuple in staging_data:
            row = dict(zip(column_names, row_tuple))
            transformed_row = transform_data(row)
            if len(table_ids) == 0 or transformed_row['Video_Id'] not in table_ids:
                insert_row(cur, conn, schema, transformed_row)
            else:
                if transformed_row['Video_Id'] in table_ids:
                    update_row(cur, conn, schema, transformed_row)
                else:
                    insert_row(cur, conn, schema, transformed_row)
        ids_in_staging = {row['Video_Id'] for row in [dict(zip(column_names, r)) for r in staging_data]}
        ids_to_delete = set(table_ids) - ids_in_staging
        if ids_to_delete:
            for video_id in ids_to_delete:
                delete_row(cur, conn, schema, video_id)
        logger.info(f"{schema} table update completed")
    except Exception as e:
        logger.error(f"Error in core_table task: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)