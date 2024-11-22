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


def generate_warehouse_query(table_name: str, table_path: str):
    if table_name[0:4] == "fact":
        query = create_fact_query(table_name, table_path)
    elif table_name[0:3] == "dim":
        query = create_dim_query(table_name, table_path)
    else:
        raise InvalidTableNameError(f"This table has an invalid name: '{table_name}'")
    return query
