import logging
logger = logging.getLogger(__name__)
table = 'yt_api'

def insert_row(cur, conn, schema, row):
    try:
        video_id = 'Video_Id'
        query = (
            f"INSERT INTO {schema}.{table} "
            "(Video_Id, Video_Title, Upload_Date, View_Count, Like_Count, Comment_Count, Duration) "
            "VALUES (%(Video_Id)s, %(Video_Title)s, %(Upload_Date)s, %(View_Count)s, %(Like_Count)s, %(Comment_Count)s, %(Duration)s);"
        )
        cur.execute(query, row)
        if schema != 'staging':
            conn.commit()
            logger.info(f"Inserted row with video_id: {row.get(video_id)} into {schema}.{table}")
    except Exception as e:
        logger.error(f"Error inserting row with video_id: {row.get(video_id)} into {schema}.{table} - {e}")
        raise

def update_row(cur, conn, schema, row):
    try:
        video_id = 'Video_Id'
        query = (
            f"UPDATE {schema}.{table} "
            "SET Video_Title = %(Video_Title)s, Upload_Date = %(Upload_Date)s, View_Count = %(View_Count)s, "
            "Like_Count = %(Like_Count)s, Comment_Count = %(Comment_Count)s, Duration = %(Duration)s "
            "WHERE Video_Id = %(Video_Id)s;"
        )
        cur.execute(query, row)
        if schema != 'staging':
            conn.commit()
            logger.info(f"Updated row with video_id: {row.get(video_id)} in {schema}.{table}")
    except Exception as e:
        logger.error(f"Error updating row with video_id: {row.get(video_id)} in {schema}.{table} - {e}")
        raise   

def delete_row(cur, conn, schema, video_id):
    try:
        query = f"DELETE FROM {schema}.{table} WHERE Video_Id = %s;"
        cur.execute(query, (video_id,))
        if schema != 'staging':
            conn.commit()
            logger.info(f"Deleted row with video_id: {video_id} from {schema}.{table}")
    except Exception as e:
        logger.error(f"Error deleting row with video_id: {video_id} from {schema}.{table} - {e}")
        raise e

# End of file