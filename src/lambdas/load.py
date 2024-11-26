from logging import getLogger
from os import environ

from boto3 import client
from pg8000.core import DatabaseError

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.create_fact_tables import create_fact_tables
    from src.utils.python.db_connections import close_db_connection, connect_to_db
    from src.utils.python.generate_warehouse_query import generate_warehouse_query
    from src.utils.python.query_db import query_db
else:
    from create_fact_tables import create_fact_tables
    from db_connections import close_db_connection, connect_to_db
    from generate_warehouse_query import generate_warehouse_query
    from query_db import query_db

logger = getLogger(__name__)


def lambda_handler(event, context):
    s3_client = client("s3")
    try:
        create_fact_tables(connect_to_db, close_db_connection)
    except DatabaseError as de:
        logger.critical(f"Failed to create fact tables: {de}")
    for table_name in sorted(event):
        query = generate_warehouse_query(table_name, event[table_name], s3_client)
        try:
            query_db(
                query,
                connect_to_db,
                close_db_connection,
                table_name,
                "totesys_warehouse_credentials",
            )
            logger.info(
                f"Successfully updated warehouse table {table_name} from path {event[table_name]}"
            )
        except DatabaseError as de:
            logger.critical(
                f"Failed database query for {table_name} at path {event[table_name]}: {de}"
            )
