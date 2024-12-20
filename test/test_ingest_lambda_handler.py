import os

os.environ["DEV_ENVIRONMENT"] = "testing"

from src.lambdas.ingest import lambda_handler
from unittest.mock import patch
from pytest import mark
from datetime import datetime
from botocore.exceptions import ClientError
from logging import CRITICAL, INFO
from src.utils.python.generate_new_entry_query import DateFormatError
from pg8000.core import DatabaseError

PATCH_PATH = "src.lambdas.ingest"

"""
Things to be patched:
    - get_last_ingest_time
    - query_db
    - write_to_s3
    - datetime.now
"""

ig_bucket_name = "test_bucket"
PATCHED_ENVIRON = {
    "ingest_bucket_name": ig_bucket_name,
    "static_address_path": "static/address.parquet",
    "static_department_path": "static/department.parquet",
}


class TestIntegration:
    @mark.it("calls s3 client is called with the correct file name, bucket and body")
    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    def test_1(self, caplog):
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
                    with patch(f"{PATCH_PATH}.s3_client") as s3_mock:
                        test_event = {"tables_to_query": ["table_name"]}

                        with patch(f"{PATCH_PATH}.parquet_data", return_value=""):
                            caplog.set_level(INFO)
                            response = lambda_handler(test_event, {})
                        expected_calls = {
                            "Bucket": "test_bucket",
                            "Key": "table_name/2024/11/13/141420987654.parquet",
                            "Body": "",
                        }
                        s3_mock.put_object.assert_called_with(**expected_calls)
        assert "Successfully retrieved last ingest time from" in caplog.text
        assert "Successfully generated SQL query for table" in caplog.text
        assert "Successfully retrieved 5 rows from table" in caplog.text
        assert "Successfully written parquet data to" in caplog.text
        assert "No new rows found for" not in caplog.text
        assert response == {"table_name": "table_name/2024/11/13/141420987654.parquet"}


class TestErrorRaisedByGetLastIngestTime:
    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it("creates a critical log if get_last_ingest_time raises ClientError")
    def test_2(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time") as client_error_mock:
            error = ClientError(
                {
                    "Error": {
                        "Code": "InternalServiceError",
                        "Message": "not our problem",
                    }
                },
                "testing",
            )
            client_error_mock.side_effect = error
            caplog.set_level(CRITICAL)
            lambda_handler({"tables_to_query": [""]}, {})
        assert "Error retrieving last ingest time from s3:" in caplog.text


class TestErrorRaisedByGenerateNewEntryQuery:
    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it(
        "creates a critical log if generate_new_entry_query raises DateFormatError"
    )
    def test_3(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time", return_value=datetime.now()):
            with patch(
                f"{PATCH_PATH}.generate_new_entry_query", side_effect=DateFormatError
            ):
                caplog.set_level(CRITICAL)
                lambda_handler({"tables_to_query": [""]}, {})
        assert "Error generating SQL query for table " in caplog.text


class TestQueryDB:
    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it("creates a critical log if query_db raises DatabaseError")
    def test_4(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time", return_value=datetime.now()):
            with patch(f"{PATCH_PATH}.generate_new_entry_query", return_value=""):
                with patch(f"{PATCH_PATH}.query_db", side_effect=DatabaseError):
                    caplog.set_level(CRITICAL)
                    lambda_handler({"tables_to_query": [""]}, {})
        assert "Error querying database with query" in caplog.text

    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it("creates a warning log if query_db returns no values")
    def test_5(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time", return_value=datetime.now()):
            with patch(f"{PATCH_PATH}.generate_new_entry_query", return_value=""):
                with patch(f"{PATCH_PATH}.query_db", return_value={"something": []}):
                    caplog.set_level(INFO)
                    lambda_handler({"tables_to_query": ["something"]}, {})
        assert "No new rows found for" in caplog.text


class TestErrorRaisedByWriteTos3:
    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it("creates a critical log if write_to_s3 raises ClientError")
    def test_6(self, caplog):
        with patch(f"{PATCH_PATH}.get_last_ingest_time", return_value=datetime.now()):
            with patch(f"{PATCH_PATH}.generate_new_entry_query", return_value=""):
                with patch(f"{PATCH_PATH}.query_db") as query_db_mock:
                    query_db_mock.return_value = {
                        "something": [
                            {
                                "id": 17,
                                "title": "Back to the Future",
                                "ten_divided_by_2": 5,
                                "rating": 10,
                                "certificate": "U",
                                "avg_rating": "2.38",
                            }
                        ]
                    }
                    with patch(f"{PATCH_PATH}.parquet_data", return_value=""):
                        with patch(f"{PATCH_PATH}.write_to_s3") as write_to_s3_mock:
                            error = ClientError(
                                {
                                    "Error": {
                                        "Code": "InternalServiceError",
                                        "Message": "not our problem",
                                    }
                                },
                                "testing",
                            )
                            write_to_s3_mock.side_effect = error
                            caplog.set_level(CRITICAL)
                            lambda_handler({"tables_to_query": ["something"]}, {})
        assert "Error writing parquet to" in caplog.text


class TestIngestDimTable:
    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it(
        "gets full table is returned when only few values is modified in the normalised dim table"
    )
    def test_7(self):
        with patch(f"{PATCH_PATH}.dt") as dt_mock:
            dt_mock.now.return_value = datetime(2024, 11, 13, 14, 14, 20, 987654)
            with patch(
                f"{PATCH_PATH}.get_last_ingest_time",
                return_value=datetime(2024, 11, 13, 14, 00, 20, 987654),
            ):
                with patch(
                    f"{PATCH_PATH}.generate_new_entry_query"
                ) as generate_new_entry_query_mock:
                    generate_new_entry_query_mock.side_effect = [
                        "first_call",
                        "second_call",
                    ]
                    with patch(f"{PATCH_PATH}.query_db") as query_db_mock:
                        query_db_mock.return_value = {
                            "test_dim_table": [
                                {
                                    "id": 17,
                                    "title": "Back to the Future",
                                    "ten_divided_by_2": 5,
                                    "rating": 10,
                                    "certificate": "U",
                                    "avg_rating": "2.38",
                                }
                            ]
                        }
                        with patch(f"{PATCH_PATH}.s3_client"):
                            test_event = {"tables_to_query": ["test_dim_table"]}

                            with patch(f"{PATCH_PATH}.parquet_data", return_value=""):
                                response = lambda_handler(test_event, {})
        assert generate_new_entry_query_mock.call_count == 2
        assert query_db_mock.call_count == 2
        call_values = generate_new_entry_query_mock.call_args_list
        call_arg_1, _ = call_values[0]
        call_arg_2, _ = call_values[1]
        assert call_arg_1 == (
            "test_dim_table",
            "2024-11-13 14:00:20.987654",
            "2024-11-13 14:14:20.987654",
        )
        assert call_arg_2 == (
            "test_dim_table",
            "2000-01-01 00:00:00.000000",
            "2024-11-13 14:14:20.987654",
        )
        call_values = query_db_mock.call_args_list
        call_arg_1, _ = call_values[0]
        call_arg_2, _ = call_values[1]
        assert call_arg_1[0] == "first_call"
        assert call_arg_2[0] == "second_call"
        assert response == {
            "test_dim_table": "test_dim_table/2024/11/13/141420987654.parquet"
        }

    @patch.dict(
        f"{PATCH_PATH}.os.environ", {"ingest_bucket_name": "test_bucket"}, clear=True
    )
    @mark.it(
        "checks if table is a normalised fact table then generate_new_entry_query and query_db is called once"
    )
    def test_8(self):
        with patch(f"{PATCH_PATH}.dt") as dt_mock:
            dt_mock.now.return_value = datetime(2024, 11, 13, 14, 14, 20, 987654)
            with patch(
                f"{PATCH_PATH}.get_last_ingest_time",
                return_value=datetime(2024, 11, 13, 14, 00, 20, 987654),
            ):
                with patch(
                    f"{PATCH_PATH}.generate_new_entry_query"
                ) as generate_new_entry_query_mock:
                    generate_new_entry_query_mock.return_value = ""
                    with patch(f"{PATCH_PATH}.query_db") as query_db_mock:
                        query_db_mock.return_value = {
                            "payment": [
                                {
                                    "id": 17,
                                    "title": "Back to the Future",
                                    "ten_divided_by_2": 5,
                                    "rating": 10,
                                    "certificate": "U",
                                    "avg_rating": "2.38",
                                }
                            ]
                        }
                        with patch(f"{PATCH_PATH}.s3_client"):
                            test_event = {"tables_to_query": ["payment"]}

                            with patch(f"{PATCH_PATH}.parquet_data", return_value=""):
                                response = lambda_handler(test_event, {})
        assert generate_new_entry_query_mock.call_count == 1
        assert query_db_mock.call_count == 1
        assert response == {"payment": "payment/2024/11/13/141420987654.parquet"}


class TestStaticEnviron:
    @patch.dict(f"{PATCH_PATH}.os.environ", PATCHED_ENVIRON, clear=True)
    @mark.it("checks if static_table_name_key is added to s3")
    def test_9(self):
        with patch(f"{PATCH_PATH}.dt") as dt_mock:
            dt_mock.now.return_value = datetime(2024, 11, 13, 14, 14, 20, 987654)
            with patch(
                f"{PATCH_PATH}.get_last_ingest_time",
                return_value=datetime(2024, 11, 13, 14, 00, 20, 987654),
            ):
                with patch(
                    f"{PATCH_PATH}.generate_new_entry_query"
                ) as generate_new_entry_query_mock:
                    generate_new_entry_query_mock.return_value = ""
                    with patch(f"{PATCH_PATH}.query_db") as query_db_mock:
                        query_db_mock.return_value = {
                            "address": [
                                {
                                    "id": 17,
                                    "title": "Back to the Future",
                                    "ten_divided_by_2": 5,
                                    "rating": 10,
                                    "certificate": "U",
                                    "avg_rating": "2.38",
                                }
                            ]
                        }
                        with patch(f"{PATCH_PATH}.s3_client") as s3_mock:
                            test_event = {"tables_to_query": ["address"]}

                            with patch(f"{PATCH_PATH}.parquet_data", return_value=""):
                                response = lambda_handler(test_event, {})
                                expected_static_call = {
                                    "Bucket": "test_bucket",
                                    "Key": "static/address.parquet",
                                    "Body": "",
                                }
                                expected_call = {
                                    "Bucket": "test_bucket",
                                    "Key": "address/2024/11/13/141420987654.parquet",
                                    "Body": "",
                                }
                                call_values = s3_mock.put_object.call_args_list
                                _, call_kwargs_1 = call_values[0]
                                _, call_kwargs_2 = call_values[1]
            assert call_kwargs_1 == expected_static_call
            assert call_kwargs_2 == expected_call
            assert response == {"address": "address/2024/11/13/141420987654.parquet"}
