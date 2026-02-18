from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = 'yt_api'
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database= 'elt_db')
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def create_table(schema):
    conn , cur = get_conn_cursor()
    if schema == 'staging':
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            Video_Id VARCHAR(11) PRIMARY KEY NOT NULL,
            Video_Title TEXT NOT NULL,
            Upload_Date TIMESTAMP NOT NULL,
            View_Count INTEGER,
            Like_Count INTEGER,
            Comment_Count INTEGER,
            Duration VARCHAR(20) NOT NULL
        );
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            Video_Id VARCHAR(11) PRIMARY KEY NOT NULL,
            Video_Title TEXT NOT NULL,
            Upload_Date TIMESTAMP NOT NULL,
            View_Count INTEGER,
            Like_Count INTEGER,
            Comment_Count INTEGER,
            Duration TIME NOT NULL
        );
        """
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    # Select Video_Id from the table; Postgres folds unquoted identifiers to lowercase
    cur.execute(f"SELECT Video_Id FROM {schema}.{table};")
    ids = cur.fetchall()
    # RealDictCursor returns dicts with lowercase keys for unquoted columns
    video_ids = [row.get('video_id') or row.get('video_id'.lower()) or row.get('Video_Id') for row in ids]
    # Filter out any None values and return the list
    return [vid for vid in video_ids if vid]