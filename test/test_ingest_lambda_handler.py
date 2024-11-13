from src.lambdas.ingest import lambda_handler
from unittest.mock import patch
from pytest import mark, fixture
from datetime import datetime
from moto import mock_aws
from os import environ
from boto3 import client


PATCH_PATH = "src.lambdas.ingest"

"""
Things to be patched:
    - get_last_ingest_time
    - query_db
    - write_to_s3
    - datetime.now
"""


@fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    environ["AWS_SECURITY_TOKEN"] = "testing"
    environ["AWS_SESSION_TOKEN"] = "testing"
    environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@fixture(scope='function')
def s3(aws_credentials):
    return client("s3")


class TestIntegration:
    @mark.it("checks if everything is fine and working")

    def test_1(self, s3):
        with patch(f"{PATCH_PATH}.dt") as dt_mock:
            dt_mock.now.return_value = datetime(2024, 11, 13, 14, 14, 20, 987654)
            with patch(
                f"{PATCH_PATH}.get_last_ingest_time",
                return_value=datetime(2024, 11, 13, 14, 00, 20, 987654),
            ):
                with patch(f"{PATCH_PATH}.query_db") as query_db_mock:
                    query_db_mock.return_value = {
                        "table_name": [
                            {
                                "id": 17,
                                "title": "Back to the Future",
                                "ten_divided_by_2": 5,
                                "rating": 10,
                                "certificate": "U",
                                "avg_rating": "2.38",
                            },
                            {
                                "id": 18,
                                "title": "Back to the Future",
                                "ten_divided_by_2": 5,
                                "rating": 10,
                                "certificate": "U",
                                "avg_rating": "2.38",
                            },
                            {
                                "id": 19,
                                "title": "Back to the Future",
                                "ten_divided_by_2": 5,
                                "rating": 10,
                                "certificate": "U",
                                "avg_rating": "2.38",
                            },
                            {
                                "id": 20,
                                "title": "Back to the Future",
                                "ten_divided_by_2": 5,
                                "rating": 10,
                                "certificate": "U",
                                "avg_rating": "2.38",
                            },
                            {
                                "id": 21,
                                "title": "Back to the Future",
                                "ten_divided_by_2": 5,
                                "rating": 10,
                                "certificate": "U",
                                "avg_rating": "2.38",
                            },
                        ]
                    }
                    with patch(f"{PATCH_PATH}.client", return_value=s3):
                        test_event = {"tables_to_query": ["table_name"]}
                        environ["bucket_name"] = "test-bucket-1234098799"
                        with mock_aws():
                            s3.create_bucket(
                                Bucket=environ["bucket_name"],
                                CreateBucketConfiguration={
                                    "LocationConstraint": "eu-west-2"
                                },
                            )
                            lambda_handler(test_event, {})
                            print(s3.list_objects_v2(Bucket=environ["bucket_name"]))
                        assert False


class TestErrorRaisedByGetLastIngestTime:
    def test_(self):
        pass


class TestErrorRaisedByGenerateNewEntryQuery:
    def test_(self):
        pass


class TestErrorRaisedByQueryDB:
    def test_(self):
        pass


class TestErrorRaisedByWriteTos3:
    def test_(self):
        pass
