from moto import mock_aws
from boto3 import client
from pandas import read_parquet, DataFrame
from os import environ
from io import BytesIO
from unittest.mock import patch
from datetime import datetime
from pytest import mark

environ["DEV_ENVIRONMENT"] = "testing"

from src.lambdas.transform import lambda_handler as transform
from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet

ig_bucket_name = "ig_bucket"
tf_bucket_name = "tf_bucket"
PATCHED_ENVIRON = {
    "ingest_bucket_name": ig_bucket_name,
    "transform_bucket_name": tf_bucket_name,
}


@mock_aws
def put_parquet_file_to_s3(filepath, bucket_name, s3_client, table_name):
    test_df = read_parquet(filepath)
    parquet_data = BytesIO()
    s3_key = f"{table_name}/yadayada.parquet"
    test_df.to_parquet(parquet_data, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_data.getvalue())


@mock_aws
@mark.it(
    "loads a single table into the warehouse, then its transformed counterpart is added to the transform bucket"
)
def test_1():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    test_df = read_parquet("test/test_data/parquet_files/transaction.parquet")
    parquet_data = BytesIO()
    test_df.to_parquet(parquet_data, index=False)
    test_key = "transaction/dddddd.parquet"
    s3_client.put_object(
        Bucket=ig_bucket_name, Key=test_key, Body=parquet_data.getvalue()
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            transform({"transaction": test_key})
    result_key = f"transaction/{test_time[0]}/{test_time[1]}/{test_time[2]}/{test_time[3]}{test_time[4]}00000000.parquet"
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, result_key)
    assert isinstance(result, DataFrame)
    assert result.columns.values.tolist() == [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id",
    ]


@mark.it(
    "constructs the corresponding data warehouse tables for the ingest tables given in the event"
)
@mock_aws
def test_2():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    for table_name in ["address", "counterparty", "currency", "design", "department"]:
        put_parquet_file_to_s3(
            f"test/test_data/parquet_files/{table_name}.parquet",
            ig_bucket_name,
            s3_client,
            table_name,
        )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            transform(
                {
                    "address": "address/yadayada.parquet",
                    "counterparty": "counterparty/yadayada.parquet",
                    "currency": "currency/yadayada.parquet",
                    "design": "design/yadayada.parquet",
                    "static_department": "department/yadayada.parquet",
                    "static_address": "address/yadayada.parquet",
                }
            )
    tf_parquet_list = [
        item["Key"]
        for item in s3_client.list_objects_v2(Bucket=tf_bucket_name)["Contents"]
    ]
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, tf_parquet_list[1])
    assert result.columns.values.tolist() == [
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
        "counterparty_id",
        "counterparty_legal_name",
    ]


@mock_aws
@mark.it(
    "doesn't construct location and department tables when only std_address and std_department are in the event"
)
def test_3():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/address.parquet",
        ig_bucket_name,
        s3_client,
        "std_address",
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/department.parquet",
        ig_bucket_name,
        s3_client,
        "std_department",
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        transform(
            {
                "std_department": "std_department/yadayada.parquet",
                "std_address": "std_address/yadayada.parquet",
            }
        )
    assert not s3_client.list_objects_v2(Bucket=tf_bucket_name)["KeyCount"]
