from os import environ
from pandas import DataFrame, isnull

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
else:
    from get_df_from_s3_parquet import get_df_from_s3_parquet


def generate_delete_from_statement(table_name: str) -> str:
    return f"DELETE FROM {table_name};\n"


def format_value(value):
    if value is None or isnull(value):
        return "NULL"
    return str(value).replace("'", "''")


def generate_insert_into_statement(
    table_name: str, columns: list[str], df: DataFrame
) -> str:
    output = f"INSERT INTO {table_name}\n"
    output += f"""    ({", ".join(columns)})\n"""
    output += "VALUES\n"
    for _, row in df.iterrows():
        row_list = [f"'{format_value(column, row[column])}'" for column in columns]
        output += f'    ({", ".join(row_list)}),\n'
    return output[:-2] + ";"


def create_dim_query(table_name: str, table_path: str, s3_client) -> str:
    """
    Generates a query to delete all entries in a table and insert values from a parquet file.
    Parameters:
    table_name: name of warehouse table to modify
    table_path: path to parquet file in ingest bucket to populate the table with
    s3_client: s3 client to access transform bucket with
    """
    if not table_name:
        raise ValueError("table_name must not be null")
    df = get_df_from_s3_parquet(s3_client, environ["transform_bucket_name"], table_path)
    columns = df.columns.values.tolist()
    sql_string = generate_delete_from_statement(
        table_name
    ) + generate_insert_into_statement(table_name, columns, df)
    return sql_string
