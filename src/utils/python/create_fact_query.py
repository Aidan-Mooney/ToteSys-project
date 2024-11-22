from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
from os import environ 
from pg8000.native import literal


def create_fact_query(table_name, table_path, s3_client):
    """
    SQL Query to check if it exists, return True/False
    If True we append to the existing fact table
    if False we need to create the fact table and insert values into table
    return query string to be used to update the table (in a separate function)
    """

    df = get_df_from_s3_parquet(
        s3_client, bucket_name=environ["transform_bucket_name"], filename=table_path
    )

    facts_cols = df.columns.values.tolist()
    cols_string = ", ".join(facts_cols)

    query = f"INSERT INTO {table_name} ({cols_string}) VALUES "

    facts_vals = df.values.tolist()
    for row in facts_vals:
        for i in range(len(row)):
            element = row[i]
            row[i] = literal(element)
        values = ", ".join(row)
        query += f"({values}),"

    return query[:-1] + ";"
