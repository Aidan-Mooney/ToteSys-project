import os
from datetime import datetime as dt
from datetime import timezone
from logging import getLogger

from boto3 import client
from botocore.exceptions import ClientError
from pg8000.core import DatabaseError

DEV_ENVIRONMENT=os.environ['DEV_ENVIRONMENT'] 
if DEV_ENVIRONMENT == 'testing':

    from src.utils.python.db_connections import close_db_connection, connect_to_db
    from src.utils.python.format_time import format_time
    from src.utils.python.generate_file_key import generate_file_key
    from src.utils.python.generate_new_entry_query import (
        DateFormatError,
        generate_new_entry_query,
    )
    from src.utils.python.get_last_ingest_time import get_last_ingest_time
    from src.utils.python.parquet_data import parquet_data
    from src.utils.python.query_db import query_db
    from src.utils.python.write_to_s3 import write_to_s3
else:
    from db_connections import close_db_connection, connect_to_db
    from format_time import format_time
    from generate_file_key import generate_file_key
    from generate_new_entry_query import DateFormatError, generate_new_entry_query
    from get_last_ingest_time import get_last_ingest_time
    from parquet_data import parquet_data
    from query_db import query_db
    from write_to_s3 import write_to_s3
    
s3_client = client("s3")
logger = getLogger(__name__)

def lambda_handler(event, context):
    """
    Structure of event:
        event = {"tables_to_query": ["table_name",...]}
    
    Process:
        - gets the start_time from get_last_ingest_time
        - end_time is the time at the start of the run
        - generates a SQL query for each table name
        - queries the database
        - converts the data to parquet
        - adds it to the s3 bucket

    Logs:
        - INFO when each function is successful
        - CRITICAL when each function fails fatally
        - WARNING when query_db returns an empty list

    """
    bucket_name = os.environ["bucket_name"]
    end_time = dt.now(timezone.utc) #add timezone or look at the context to see if there's time
    end_time_str = format_time(end_time)
    table_names = event["tables_to_query"]
    file_key_list = []
    for table_name in table_names:
        try:
            start_time = get_last_ingest_time(bucket_name, table_name)
            start_time_str = format_time(start_time)
            logger.info(f"Successfully retrieved last ingest time from {bucket_name} for table '{table_name}'")
        except (AttributeError, ClientError) as e:
            logger.critical(f"Error retrieving last ingest time from s3:\n{e}")
            break
        try:
            query_string = generate_new_entry_query(table_name, start_time_str, end_time_str)
            logger.info(f"Successfully generated SQL query for table '{table_name}' between {start_time_str} and {end_time_str}")
        except DateFormatError as e:
            logger.critical(f"Error generating SQL query for table '{table_name}' between {start_time_str} and {end_time_str}:\n{e}")
            break
        try:
            new_rows = query_db(
                query_string, connect_to_db, close_db_connection, table_name
            )
            logger.info(f"Successfully retrieved {len(new_rows[table_name])} rows from table '{table_name}'")
        except DatabaseError as e:
            logger.critical(f"Error querying database with query: '{query_string}':\n{e}")
            break
        if new_rows[table_name]:
            file_key = generate_file_key(table_name, end_time)
            file_key_list.append(file_key)
            new_rows_parquet = parquet_data(new_rows)
            try:
                write_to_s3(s3_client, bucket_name, file_key, new_rows_parquet)
                logger.info(f"Successfully written parquet data to {bucket_name}/{file_key}'")
            except ClientError as e:
                logger.critical(f"Error writing parquet to {bucket_name}/{file_key}:\n{e}")
                break
        else:
            logger.warning(f"No new rows found for {table_name} between {start_time_str} and {end_time_str}")
        
        return {"files_added": file_key_list}
