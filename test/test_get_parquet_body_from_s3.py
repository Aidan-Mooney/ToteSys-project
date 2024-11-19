from moto import mock_aws
from pytest import mark, fixture
from boto3 import client
from os import environ
from src.transform_utils.get_parquet_body_from_s3 import get_parquet_body_from_s3


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
def test_1(s3_client):
    bucket_name = "testy-b"
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    test_body = "lalalalal"
    s3_client.put_object(Bucket=bucket_name, Key="file.parquet", Body=test_body)
    result = get_parquet_body_from_s3("file.parquet", bucket_name)
    assert result == test_body


# add a test that actually uses parquet files and pandas dataframes
