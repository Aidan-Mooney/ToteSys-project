from os import environ

DEV_ENVIRONMENT = environ["DEV_ENVIRONMENT"]
if DEV_ENVIRONMENT == "testing":
    from src.utils.python.create_fact_query import create_fact_query
    from src.utils.python.create_dim_query import create_dim_query
else:
    from create_fact_query import create_fact_query
    from create_dim_query import create_dim_query


class InvalidTableNameError(Exception):
    pass


def generate_warehouse_query(table_name: str, table_path: str, s3_client):
    """
    Returns the database query string for updating the table table_name. See create_fact_query and create_dim_query for specific queries.

    PARAMETERS
    table_name: str = name of the warehouse table to be queried
    table_path: str = path to the data for corresponding table in transform bucket
    s3_client: obj = s3_client to be used by create_fact_query and create_dim_query to access the parquet files
    """
    if table_name[0:4] == "fact":
        query = create_fact_query(table_name, table_path, s3_client)
    elif table_name[0:3] == "dim":
        query = create_dim_query(table_name, table_path, s3_client)
    else:
        raise InvalidTableNameError(f"This table has an invalid name: '{table_name}'")
    return query
