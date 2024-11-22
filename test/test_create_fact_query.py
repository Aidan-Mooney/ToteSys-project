from src.utils.python.create_fact_query import create_fact_query
from unittest.mock import patch
from pandas import DataFrame, read_parquet

PATCH_PATH = "src.utils.python.create_fact_query"


@patch.dict(
    f"{PATCH_PATH}.environ", {"transform_bucket_name": "test_bucket"}, clear=True
)
def test_columns_can_be_used_in_sql_statement():
    age = [5, 66, 11, 22, 145]
    first_name = ["matt", "matt2", "aiden", "anthony", "chris"]
    last_name = [True, False, True, True, False]
    column_names = ["age", "first_name", "last_name"]
    data = list(zip(age, first_name, last_name))
    df = DataFrame(data, columns=column_names)

    with patch(f"{PATCH_PATH}.get_df_from_s3_parquet") as df_mock:
        df_mock.return_value = df
        result = create_fact_query("test_table_name", "", "s3_client")

    assert (
        result
        == "INSERT INTO test_table_name (age, first_name, last_name) VALUES (5, 'matt', TRUE),(66, 'matt2', FALSE),(11, 'aiden', TRUE),(22, 'anthony', TRUE),(145, 'chris', FALSE);"
    )


@patch.dict(
    f"{PATCH_PATH}.environ", {"transform_bucket_name": "test_bucket"}, clear=True
)
def test_date_time_can_be_used():
    df1 = read_parquet("test/test_data/parquet_files/fact_payment.parquet")[
        ["transaction_id", "created_date", "created_time", "paid"]
    ]
    df2 = df1.head(2)
    with patch(f"{PATCH_PATH}.get_df_from_s3_parquet") as df_mock:
        df_mock.return_value = df2
        result = create_fact_query("fact_payment", "", "s3_client")

        assert result == "INSERT INTO fact_payment (transaction_id, created_date, created_time, paid) VALUES (15850, '2024-11-18', '09:01:10.169000', TRUE),(15855, '2024-11-18', '09:52:10.245000', TRUE);"
