from os import environ 
from pg8000.native import literal

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
else:
    from get_df_from_s3_parquet import get_df_from_s3_parquet


def create_fact_query(table_name: str, table_path: str, s3_client) -> str:
    """
    This creates a query for fact tables

    Input:
        - table_name: gives the name of the fact table
        - table_path: gives the file_key for the fact table
        - s3_client: client for s3

    Process:
        - creates fact tables if they do not exist
        - it gets the panda dataframe from corresponding fact_table and file_key
        - fact_cols gets the column names of the fact table
        - adds the fact_cols to the beginning of the insert query statement
        - fact_vals gets a list of values/rows of the fact table
        - finally appends values/rows to the query in the correct format
    
    Output:
        - return a query string to update fact table
    """

    df = get_df_from_s3_parquet(
        s3_client, bucket_name=environ["transform_bucket_name"], filename=table_path
    )

    fact_cols = df.columns.values.tolist()
    cols_string = ", ".join(fact_cols)

    query = f"INSERT INTO {table_name} ({cols_string}) VALUES "

    facts_vals = df.values.tolist()
    for row in facts_vals:
        for i in range(len(row)):
            element = row[i]
            row[i] = literal(element)
        values = ", ".join(row)
        query += f"({values}),"

    return query[:-1] + ";"
