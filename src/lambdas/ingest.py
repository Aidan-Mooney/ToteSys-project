from utils.get_last_ingest_time import get_last_ingest_time
from utils.generate_new_entry_query import generate_new_entry_query, DateFormatError
from utils.db_connections import connect_to_db, close_db_connection
from utils.generate_file_key import generate_file_key
from utils.format_time import format_time
from utils.query_db import query_db
from datetime import now
from boto3 import client
from os import environ
from pg8000.core import DatabaseError
from botocore.exceptions import ClientError

s3_client = client("s3")
bucket_name = environ["bucket_name"]


def lambda_handler(event, context):
    """
    This function should wrap anything in try:...except: blocks that uses an external service
    This function should log things (what things??)
    Structure of event:
        event = {"tables_to_query": [""]}
    This event will be hard-coded(?) with the names of the tables we'd like to query. (Possible implementation: the event would be stored in TerraForm and passed in by the step function.)
    """
    end_time = format_time(now())
    table_names = event["tables_to_query"]
    for table_name in table_names:
        try:
            start_time = format_time(get_last_ingest_time(bucket_name, table_name))
            # [INFO] log successful retrieval of ingest time
        except ClientError as e:
            # [CRITICAL] log an error reading from s3 {e}
            break
        try:
            query_string = generate_new_entry_query(table_name, start_time, end_time)
            # [INFO] log successful query generation
        except DateFormatError as e:
            # [CRITICAL] log that the time can't be read and the function has failed {e}
            break
        try:
            new_rows = query_db(
                query_string, connect_to_db, close_db_connection, table_name
            )
            # [INFO] log successful databse query
        except DatabaseError as e:
            # [CRITICAL] log that the time can't be read and the function has failed {e}
            break
        if new_rows[table_name]:
            file_key = generate_file_key(table_name, end_time)
            # write_parquet_to_s3(s3_client, bucket_name, file_key, new_rows)
            # [INFO] log writing to s3 bucket, file_key

        # [INFO] log no new rows found for table_name between start_time and end_time
