from pytest import mark, fixture
from moto import mock_aws
from boto3 import client
from os import environ
from src.utils.python.write_to_s3 import write_to_s3

TEST_FILE_PATH = "test/test_data"
TEST_BUCKET = "test_bucket"


@fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    environ["AWS_SECURITY_TOKEN"] = "testing"
    environ["AWS_SESSION_TOKEN"] = "testing"
    environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@fixture()
def s3_client():
    return client("s3")


@mock_aws
@mark.it("Puts object in the bucket with correct filename")
def test_1(s3_client):
    s3_client.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open(f"{TEST_FILE_PATH}/file1.txt") as f:
        data = f.read()
    test_filename = "test.txt"
    write_to_s3(s3_client, TEST_BUCKET, test_filename, data)
    result = s3_client.list_objects_v2(Bucket=TEST_BUCKET)["Contents"]
    assert len(result) == 1
    assert result[0]["Key"] == test_filename


@mock_aws
@mark.it("Puts object in the bucket with correct contents")
def test_2(s3_client):
    s3_client.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open(f"{TEST_FILE_PATH}/file1.txt") as f:
        data = f.read()
    test_filename = "test.txt"
    write_to_s3(s3_client, TEST_BUCKET, test_filename, data)
    result = (
        s3_client.get_object(Bucket=TEST_BUCKET, Key=test_filename)["Body"]
        .read()
        .decode("utf-8")
    )
    assert result == data


@mock_aws
@mark.it("Returns None")
def test_3(s3_client):
    s3_client.create_bucket(
        Bucket=TEST_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open(f"{TEST_FILE_PATH}/file1.txt") as f:
        data = f.read()
    test_filename = "test.txt"
    result = write_to_s3(s3_client, TEST_BUCKET, test_filename, data)
    assert result is None
