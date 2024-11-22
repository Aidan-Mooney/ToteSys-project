from os import environ
from pandas import DataFrame, isnull

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


def generate_drop_table_statement(table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {table_name};\n"


def generate_create_table_statement(table_name: str, col_dict: dict[str]) -> str:
    if not col_dict:
        raise ValueError("column_dict must be non-empty")
    output = f"CREATE TABLE {table_name} (\n"
    for column, dtype in col_dict.items():
        if column[-3:] == "_id" and table_name != "dim_date":
            dtype = "INT"
        output += f"    {column} {dtype},\n"
    output += ");\n"
    return output


def generate_insert_into_statement(
    table_name: str, columns: list[str], df: DataFrame
) -> str:
    output = f"INSERT INTO {table_name}\n"
    output += f"    ({', '.join(columns)})\n"
    output += "VALUES\n"
    for _, row in df.iterrows():
        row_list = [
            str(row[column]).replace("'", "''")
            if row[column] is not None and not isnull(row[column])
            else "NULL"
            for column in columns
        ]
        output += f'    ({", ".join(row_list)})\n'
    return output + ";"


def create_dim_query(table_name: str, table_path: str, s3_client) -> str:
    if not table_name:
        raise ValueError("table_name must not be null")
    df = get_df_from_s3_parquet(s3_client, environ["transform_bucket_name"], table_path)
    columns = df.columns.values.tolist()
    if table_name == "dim_date":
        sql_data_type_dict = {
            "date_id": "DATE",
            "year": "INT",
            "month": "INT",
            "day": "INT",
            "day_of_week": "INT",
            "day_name": "VARCHAR(100)",
            "month_name": "VARCHAR(100)",
            "quarter": "INT",
        }
    else:
        pd_data_type_dict = {col_name: str(df.dtypes[col_name]) for col_name in columns}
        sql_data_type_dict = {
            col_name: pandas_to_sql_dtype[value]
            for col_name, value in pd_data_type_dict.items()
        }
    sql_string = (
        generate_drop_table_statement(table_name)
        + generate_create_table_statement(table_name, sql_data_type_dict)
        + generate_insert_into_statement(table_name, columns, df)
    )
    return sql_string
