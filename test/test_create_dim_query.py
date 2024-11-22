from os import environ
from unittest.mock import patch
from pandas import read_parquet
from pytest import mark, raises

environ["DEV_ENVIRONMENT"] = "testing"
from src.utils.python.create_dim_query import (
    create_dim_query,
    generate_drop_table_statement,
    generate_create_table_statement,
    generate_insert_into_statement,
)

PATCHPATH = "src.utils.python.create_dim_query"
PATCHDICT = {"transform_bucket_name": "test"}


# @mark.it("raises ValueError if table_name is empty")
# def test_3(self):
#     test_table_name = ""
#     test_column_dict = {
#         "col_id": "INT",
#         "col_name": "VARCHAR(100)",
#         "col_bool": "BOOL",
#         "col_time": "TIME",
#         "col_datetime": "DATETIME",
#         "col_price": "NUMERIC",
#     }
#     with raises(ValueError) as e:
#         generate_create_table_statement(test_table_name, test_column_dict)
#     assert str(e.value) == "table_name and all columns must not be null"


@mark.context("Integration tests")
class TestIntegrationTests:
    @mark.it("generates correct sql query to drop and replace a dim table")
    def test_1(self):
        with patch.dict(environ, PATCHDICT, clear=True):
            with patch(f"{PATCHPATH}.get_df_from_s3_parquet") as get_df_mock:
                with open("test/test_data/parquet_files/dim_staff.parquet", "rb") as f:
                    get_df_mock.return_value = read_parquet(f)
                result = create_dim_query("staff", "", "")
        with open("test/test_data/warehouse_queries/dim_staff.sql", "r") as f:
            expected = f.read()
        assert result == expected


@mark.context("testing generate_drop_table_statement")
class TestGenerateDropTableStatementFunction:
    @mark.it("produces drop table statement with correct table_name")
    def test_1(self):
        test_table_name = "test_name"
        expected = "DROP TABLE IF EXISTS test_name;\n"
        result = generate_drop_table_statement(test_table_name)
        assert result == expected

    @mark.it("ends with semicolon plus newline character")
    def test_2(self):
        test_table_name = "test_name"
        result = generate_drop_table_statement(test_table_name)
        assert result[-2:] == ";\n"


@mark.context("testing generate_create_table_statement")
class TestGenerateCreateTableStatementFunction:
    @mark.it("produces create table statement with correct column names and datatypes")
    def test_1(self):
        test_table_name = "test_name"
        test_column_dict = {
            "col_id": "INT",
            "col_name": "VARCHAR(100)",
            "col_bool": "BOOL",
            "col_time": "TIME",
            "col_datetime": "DATETIME",
            "col_price": "NUMERIC",
        }
        result = generate_create_table_statement(test_table_name, test_column_dict)
        expected = "CREATE TABLE test_name (\n    col_id INT,\n    col_name VARCHAR(100),\n    col_bool BOOL,\n    col_time TIME,\n    col_datetime DATETIME,\n    col_price NUMERIC,\n);\n"
        assert result == expected

    @mark.it("ends with semicolon plus newline character")
    def test_2(self):
        test_table_name = "test_name"
        test_column_dict = {
            "col_id": "INT",
            "col_name": "VARCHAR(100)",
            "col_bool": "BOOL",
            "col_time": "TIME",
            "col_datetime": "DATETIME",
            "col_price": "NUMERIC",
        }
        result = generate_create_table_statement(test_table_name, test_column_dict)
        assert result[-2:] == ";\n"

    @mark.it("raises ValueError if column_dict is empty")
    def test_3(self):
        test_table_name = "table"
        test_column_dict = {}
        with raises(ValueError) as e:
            generate_create_table_statement(test_table_name, test_column_dict)
        assert str(e.value) == "column_dict must be non-empty"


class TestGenerate:
    print("hello")
