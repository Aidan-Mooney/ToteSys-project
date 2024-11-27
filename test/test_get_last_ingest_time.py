from pytest import mark, fixture
from boto3 import client
from moto import mock_aws
from unittest.mock import patch
from datetime import datetime
from os import environ
from src.utils.python.get_last_ingest_time import (
    get_latest_filename,
    get_last_ingest_time,
)

TEST_DATA_PATH = "test/test_data"
TEST_BUCKET = "test-bucket3141"


@fixture
def s3_client():
    s3 = client("s3")
    return s3


@fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    environ["AWS_SECURITY_TOKEN"] = "testing"
    environ["AWS_SESSION_TOKEN"] = "testing"
    environ["AWS_DEFAULT_REGION"] = "eu-west-2"


class Testget_latest_filename:
    @mock_aws
    @mark.it("Retrieves the name of the file which has the highest time")
    def test_1(self, s3_client):
        s3_client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with open(f"{TEST_DATA_PATH}/file1.txt") as f:
            body = f.read()
        s3_client.put_object(
            Body=body, Bucket=TEST_BUCKET, Key="test_table/2024/11/11/165302"
        )
        with open(f"{TEST_DATA_PATH}/file2.txt") as f:
            body = f.read()
        s3_client.put_object(
            Body=body, Bucket=TEST_BUCKET, Key="test_table/2024/11/11/165514"
        )
        result = get_latest_filename(s3_client, TEST_BUCKET, "test_table")
        expected = "2024/11/11/165514"
        assert result == expected

    @mock_aws
    @mark.it("Ignores files which have the wrong table_name")
    def test_2(self, s3_client):
        s3_client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with open(f"{TEST_DATA_PATH}/file1.txt") as f:
            body = f.read()
        s3_client.put_object(
            Body=body, Bucket=TEST_BUCKET, Key="test_table/2024/11/11/165302"
        )
        with open(f"{TEST_DATA_PATH}/file2.txt") as f:
            body = f.read()
        s3_client.put_object(
            Body=body, Bucket=TEST_BUCKET, Key="test_table/2024/11/11/165514"
        )
        with open(f"{TEST_DATA_PATH}/file1.txt") as f:
            body = f.read()
        s3_client.put_object(
            Body=body, Bucket=TEST_BUCKET, Key="test_table2/2024/11/11/175502"
        )
        result = get_latest_filename(s3_client, TEST_BUCKET, "test_table")
        expected = "2024/11/11/165514"
        assert result == expected

    @mock_aws
    @mark.it("Returns none if bucket is empty")
    def test_3(self, s3_client):
        s3_client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = get_latest_filename(s3_client, TEST_BUCKET, "test_table")
        expected = None
        assert result == expected


class Testget_last_ingest_time:
    @mark.it(
        "Returns a datetime object with the correct year, month, day, hour, minute, second and microsecond"
    )
    def test_1(self):
        test_filename = "2024/11/11/165514999999"
        with patch(
            "src.utils.python.get_last_ingest_time.get_latest_filename",
            return_value=test_filename,
        ):
            result = get_last_ingest_time(TEST_BUCKET, "")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 11
        assert result.day == 11
        assert result.hour == 16
        assert result.minute == 55
        assert result.second == 14
        assert result.microsecond == 999999

    @mark.it("Returns None if no filename exists")
    def test_2(self):
        test_filename = None
        with patch(
            "src.utils.python.get_last_ingest_time.get_latest_filename",
            return_value=test_filename,
        ):
            result = get_last_ingest_time(TEST_BUCKET, "")
        assert result is None


class Testintegration:
    @mock_aws
    @mark.it(
        "Returns the correct datetime object corresponding to the filename of a file in an s3 bucket"
    )
    def test_1(self, s3_client):
        s3_client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with open(f"{TEST_DATA_PATH}/file1.txt") as f:
            body = f.read()
        s3_client.put_object(
            Body=body, Bucket=TEST_BUCKET, Key="test_table/2025/02/15/025322999999"
        )
        result = get_last_ingest_time(TEST_BUCKET, "test_table")
        assert isinstance(result, datetime)
        assert result.year == 2025
        assert result.month == 2
        assert result.day == 15
        assert result.hour == 2
        assert result.minute == 53
        assert result.second == 22
        assert result.microsecond == 999999

    @mock_aws
    @mark.it("Returns None when no file is found.")
    def test_2(self, s3_client):
        s3_client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        result = get_last_ingest_time(TEST_BUCKET, "test_table")
        assert result is None
