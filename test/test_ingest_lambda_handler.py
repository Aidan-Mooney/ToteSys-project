from src.lambdas.ingest import lambda_handler
from unittest.mock import patch
from pytest import mark, fixture, raises
from datetime import datetime
from os import environ
from boto3 import client
from botocore.exceptions import ClientError
from logging import CRITICAL
from src.utils.generate_new_entry_query import DateFormatError
from pg8000.core import DatabaseError



PATCH_PATH = "src.lambdas.ingest"

"""
Things to be patched:
    - get_last_ingest_time
    - query_db
    - write_to_s3
    - datetime.now
"""


@fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    environ["AWS_SECURITY_TOKEN"] = "testing"
    environ["AWS_SESSION_TOKEN"] = "testing"
    environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@fixture(scope="function")
def s3(aws_credentials):
    return client("s3")


class TestIntegration:
    @mark.it("s3 client is called with the correct file name, bucket and body")
    def test_1(self, s3):
        with patch(f"{PATCH_PATH}.dt.datetime") as dt_mock:
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
                    with patch(f"{PATCH_PATH}.s3_client") as s3_mock:
                        test_event = {"tables_to_query": ["table_name"]}
                        environ["bucket_name"] = "test_bucket"
                        with patch(f"{PATCH_PATH}.parquet_data", return_value=""):
                            lambda_handler(test_event, {})
                        expected_calls = {
                            "Bucket": "test_bucket",
                            "Key": "table_name/2024/11/13/141420987654.parquet",
                            "Body": "",
                        }
                        s3_mock.put_object.assert_called_with(**expected_calls)


class TestErrorRaisedByGetLastIngestTime:
    @mark.it("creates a critical log if get_last_ingest_time raises ClientError")
    def test_2(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time") as client_error_mock:
            client_error_mock.return_value = ClientError
            caplog.set_level(CRITICAL)
            lambda_handler({'tables_to_query':['']},{})
            assert "Error retrieving last ingest time from s3:" in caplog.text


class TestErrorRaisedByGenerateNewEntryQuery:
    @mark.it("creates a critical log if generate_new_entry_query raises DateFormatError")
    def test_3(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time", return_value=datetime.now()):
            with patch(f"{PATCH_PATH}.generate_new_entry_query", side_effect=DateFormatError):
                caplog.set_level(CRITICAL)
                lambda_handler({'tables_to_query':['']},{})
                assert "Error generating SQL query for table " in caplog.text

class TestErrorRaisedByQueryDB:
    @mark.it("creates a critical log if query_db raises DatabaseError")
    def test_4(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time", return_value=datetime.now()):
            with patch(f"{PATCH_PATH}.generate_new_entry_query", return_value=""):
                with patch(f"{PATCH_PATH}.query_db", side_effect=DatabaseError):
                    caplog.set_level(CRITICAL)
                    lambda_handler({'tables_to_query':['']},{})
                    assert "Error querying database with query" in caplog.text

# class TestErrorRaisedByWriteTos3:
#     @mark.it('createsa critical log if write_to_s3 raises ClientError')
#     def test_(self):
#         pass
