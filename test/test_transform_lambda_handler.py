import os
from boto3 import client
from moto import mock_aws
from unittest.mock import patch
import pytest
import json

os.environ["DEV_ENVIRONMENT"] = "testing"
os.environ["FULL_LIST"] = '{"default": "default"}'

from src.lambdas.transform import lambda_handler


class TestTransformLambdaHandler:
    @pytest.fixture(scope="function", autouse=True)
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
        os.environ["ingest_bucket_name"] = "test_ingest_bucket"
        os.environ["transform_bucket_name"] = "test_transform_bucket"

    @pytest.fixture()
    def s3_client(self):
        return client("s3")

    @mock_aws
    def test_returns_success_message(self):
        tables = []
        event = {"tables_added": tables}
        context = {}
        result = lambda_handler(event, context)
        success_message = {"result": "success"}
        assert result["result"] == "success"

    @mock_aws
    def test_accessing_latest_files_of_tables_past_into_event(self, s3_client):
        tables = ["test_table"]
        event = {"tables_added": tables}
        context = {}
        test_key_1 = "test_table/2025/02/15/025322999999.parquet"
        test_key_2 = "test_table/2025/03/15/025322999999.parquet"
        s3_client.create_bucket(
            Bucket="test_ingest_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.put_object(Bucket="test_ingest_bucket", Key=test_key_1, Body="data")
        s3_client.put_object(Bucket="test_ingest_bucket", Key=test_key_2, Body="data")
        result = lambda_handler(event, context)
        assert result["tables"] == {"test_table": "2025/03/15/025322999999.parquet"}

    @mock_aws
    def test_only_tables_passed_in_are_returned(self, s3_client):
        tables = ["test_table"]
        event = {"tables_added": tables}
        context = {}
        test_key_1 = "test_table/2025/02/15/025322999999.parquet"
        test_key_2 = "test_table_2/2025/03/15/025322999999.parquet"
        s3_client.create_bucket(
            Bucket="test_ingest_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.put_object(Bucket="test_ingest_bucket", Key=test_key_1, Body="data")
        s3_client.put_object(Bucket="test_ingest_bucket", Key=test_key_2, Body="data")
        result = lambda_handler(event, context)
        assert result["tables"] == {"test_table": "2025/02/15/025322999999.parquet"}

    @mock_aws
    def test_related_tables_are_editted(self, s3_client):
        test_list = {"test": "testing", "staff": "department", "conterparty": "address"}
        os.environ["FULL_LIST"] = json.dumps(test_list)
        tables = ["test"]
        event = {"tables_added": tables}
        context = {}
        test_key_1 = "test/2025/02/15/025322999999.parquet"
        test_key_2 = "testing/2025/03/15/025322999999.parquet"
        s3_client.create_bucket(
            Bucket="test_ingest_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.put_object(Bucket="test_ingest_bucket", Key=test_key_1, Body="data")
        s3_client.put_object(Bucket="test_ingest_bucket", Key=test_key_2, Body="data")
        result = lambda_handler(event, context)
        assert result["tables"] == {
            "test": "2025/02/15/025322999999.parquet",
            "testing": "2025/03/15/025322999999.parquet",
        }
