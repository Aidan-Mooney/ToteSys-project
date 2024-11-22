from os import environ

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
else:
    from get_df_from_s3_parquet import get_df_from_s3_parquet

pandas_to_sql_dtype = {
    "int64": "INT",
    "object": "VARCHAR(100)",
    "datetime64[ns]": "DATETIME",
    "float64": "NUMERIC",
    "bool": "BOOL",
    "timedelta64[us]": "TIME",
}


def generate_create_table_statement(table_name, col_dict):
    output = f"CREATE TABLE {table_name} (\n"
    for column, dtype in col_dict.items():
        output += f"    {column} {dtype},\n"
    output += ");\n"
    return output


def generate_insert_into_statement(table_name, columns, df):
    output = f"INSERT INTO {table_name}\n"
    output += f"    ({', '.join(columns)})\n"
    output += "VALUES\n"
    for _, row in df.iterrows():
        output += f'    ({", ".join([str(row[column]) if row[column] is not None else "NULL" for column in columns])})\n'
    return output


def create_dim_query(table_name, table_path, s3_client):
    df = get_df_from_s3_parquet(s3_client, environ["transform_bucket_name"], table_path)
    columns = df.columns.values.tolist()
    pd_data_type_dict = {col_name: str(df.dtypes[col_name]) for col_name in columns}
    sql_data_type_dict = {
        col_name: pandas_to_sql_dtype[value]
        for col_name, value in pd_data_type_dict.items()
    }
    sql_string = f"""
DROP TABLE IF EXISTS {table_name};
{generate_create_table_statement(table_name, sql_data_type_dict)}
{generate_insert_into_statement(table_name, columns, df)};
    """
    return sql_string
