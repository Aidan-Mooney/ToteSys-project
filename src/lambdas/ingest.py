from src.utils.get_last_ingest_time import get_last_ingest_time
from src.utils.generate_new_entry_query import generate_new_entry_query, DateFormatError
from src.utils.db_connections import connect_to_db, close_db_connection
from src.utils.generate_file_key import generate_file_key
from src.utils.format_time import format_time
from src.utils.query_db import query_db
from src.utils.parquet_data import parquet_data
from src.utils.write_to_s3 import write_to_s3
import datetime as dt
from boto3 import client
from os import environ
from pg8000.core import DatabaseError
from botocore.exceptions import ClientError
from logging import getLogger


s3_client = client("s3")
bucket_name = environ["bucket_name"]
logger = getLogger(__name__)

def lambda_handler(event, context):
    """
    This function should wrap anything in try:...except: blocks that uses an external service
    This function should log things (what things??)
    Structure of event:
        event = {"tables_to_query": [""]}
    This event will be hard-coded(?) with the names of the tables we'd like to query. (Possible implementation: the event would be stored in TerraForm and passed in by the step function.)
    """
    end_time = format_time(dt.now())
    table_names = event["tables_to_query"]
    for table_name in table_names:
        try:
            start_time = format_time(get_last_ingest_time(bucket_name, table_name))
            logger.info(f"Successfully retrieved last ingest time from {bucket_name} for table '{table_name}'")
        except ClientError as e:
            logger.critical(f"Error retrieving last ingest time from s3:\n{e}")
            break
        try:
            query_string = generate_new_entry_query(table_name, start_time, end_time)
            logger.info(f"Successfully generated SQL query for table '{table_name}' between {start_time} and {end_time}")
        except DateFormatError as e:
            logger.critical(f"Error generating SQL query for table '{table_name}' between {start_time} and {end_time}:\n{e}")
            break
        try:
            new_rows = query_db(
                query_string, connect_to_db, close_db_connection, table_name
            )
            print(new_rows)
            logger.info(f"Successfully retrieved {len(new_rows[table_name])} rows from table '{table_name}'")
        except DatabaseError as e:
            logger.critical(f"Error querying database with {query_string}:\n{e}")
            break
        if new_rows[table_name]:
            file_key = generate_file_key(table_name, end_time)
            new_rows_parquet = parquet_data(new_rows)
            try:
                write_to_s3(s3_client, bucket_name, file_key, new_rows_parquet)
                logger.info(f"Successfully written parquet data to {bucket_name}/{file_key}'")
            except ClientError as e:
                logger.critical(f"Error writing parquet to {bucket_name}/{file_key}:\n{e}")
                break
        logger.warning(f"no new rows found for {table_name} between {start_time} and {end_time}")
