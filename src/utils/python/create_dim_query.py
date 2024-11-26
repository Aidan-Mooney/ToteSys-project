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
    value_rows = []
    for _, row in df.iterrows():
        row_list = [f"'{format_value(row[column])}'" for column in columns]
        value_rows.append(f'    ({", ".join(row_list)})')
    output += ",\n".join(value_rows)
    output += f"\nON CONFLICT ({table_name[4:]}_id) DO UPDATE\nSET\n"
    output += ",\n".join(
        [
            f"    {column} = EXCLUDED.{column}"
            for column in columns
            if column[-3:] != "_id"
        ]
    )
    return output + ";"


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
    return generate_insert_into_statement(table_name, columns, df)
