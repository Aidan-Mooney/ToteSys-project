from os import environ
from pandas import DataFrame

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
else:
    from get_df_from_s3_parquet import get_df_from_s3_parquet


def format_value(value: str) -> str:
    """
    Doubles-up apostrophes in values so that they are escaped in the SQL query.
    Other modifications to values which aren't made in the data warehouse should be added to this function.

    :param value: string to be modified
    """
    return str(value).replace("'", "''")


def generate_insert_into_statement(
    table_name: str, columns: list[str], df: DataFrame
) -> str:
    """
    Return an SQL query to upsert dim tables with new rows. Contains the following components:
    - INSERT INTO table_name (all columns given in parameter)
    - VALUES (rows in the dataframe)
    - ON CONFLICT (id_column) DO UPDATE SET other_column = EXCLUDED.other_column (for all columns other than the id column)
        - if a new row is entered with the same id, that row is replaced in the dim_table.
        - this 'upserting' is necessary because of foreign key constraints from the fact tables which don't allow deletion of rows from the dim tables.

    These queries are formatted to be easily read by people.

    :param table_name: name of the warehouse table being modified
    :param columns:  a list containing the names of the columns to modify. This must be a subset of the columns of df.
    :param df: a pandas DataFrame object containing the data to upsert.

    :returns INSERT INTO statement:
    """
    output = f"INSERT INTO {table_name}\n"
    output += f"""    ({", ".join(columns)})\n"""
    output += "VALUES\n"
    value_rows = []
    for _, row in df.iterrows():
        row_list = []
        for column in columns:
            formatted_val = format_value(row[column])
            if formatted_val == "NULL":
                row_list.append(formatted_val)
            else:
                row_list.append(f"'{formatted_val}'")
        value_rows.append(f'    ({", ".join(row_list)})')
    output += ",\n".join(value_rows)
    output += f"\nON CONFLICT ({table_name[4:]}_id) DO UPDATE\nSET\n"
    output += ",\n".join(
        [
            f"    {column} = EXCLUDED.{column}"
            for column in columns
            if column != f"{table_name[4:]}_id"
        ]
    )
    return output + ";"


def create_dim_query(table_name: str, table_path: str, s3_client) -> str:
    """
    Generates a query to delete all entries in a table and insert values from a parquet file
    :param table_name: name of warehouse table to modify
    :param table_path: path to parquet file in ingest bucket to populate the table with
    :param s3_client: s3 client to access transform bucket with
    """
    if not table_name:
        raise ValueError("table_name must not be null")
    df = get_df_from_s3_parquet(s3_client, environ["transform_bucket_name"], table_path)
    columns = df.columns.values.tolist()
    return generate_insert_into_statement(table_name, columns, df)
