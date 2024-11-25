from src.utils.python.create_fact_tables import create_fact_tables
from src.utils.python.query_db import query_db
from src.utils.python.db_connections import connect_to_db, close_db_connection
from src.utils.python.generate_warehouse_query import generate_warehouse_query

from boto3 import client

def lambda_handler(event, context):

    s3_client = client('s3')

    create_fact_tables(connect_to_db, close_db_connection)

    for table_name in event:
        
        query = generate_warehouse_query(table_name, event[table_name], s3_client)

        query_db(query, connect_to_db, close_db_connection, table_name)
