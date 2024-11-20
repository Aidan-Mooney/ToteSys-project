from moto import mock_aws
from pytest import fixture, mark
from boto3 import client
from os import environ
from io import BytesIO
from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
from pandas import DataFrame, read_parquet

TEST_BUCKET = "test_bucket"


@fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    environ["AWS_SECURITY_TOKEN"] = "testing"
    environ["AWS_SESSION_TOKEN"] = "testing"
    environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@fixture
def s3_client():
    return client("s3")


@mock_aws
@mark.it(
    "Returns pandas DataFrame when passed valid bucket name and filename combination"
)
def test_1(s3_client):
    bucket_name = TEST_BUCKET
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    test_df = read_parquet("test/test_data/parquet_files/transaction.parquet")
    parquet_data = BytesIO()
    test_df.to_parquet(parquet_data, index=False)
    s3_client.put_object(
        Bucket=TEST_BUCKET, Key="file.parquet", Body=parquet_data.getvalue()
    )
    result = get_df_from_s3_parquet(s3_client, TEST_BUCKET, "file.parquet")
    assert isinstance(result, DataFrame)
    cols = result.columns.values.tolist()
    assert cols == [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id",
        "created_at",
        "last_updated",
    ]
