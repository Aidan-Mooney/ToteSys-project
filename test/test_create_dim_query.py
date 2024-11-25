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


def parquet_filepath(table_name):
    return f"test/test_data/parquet_files/{table_name}.parquet"


def sql_filepath(filename):
    return f"test/test_data/warehouse_queries/{filename}.sql"


@mark.context("Testing create_dim_query")
class TestIntegrationTests:
    @mark.it("raises ValueError if table_name left blank")
    def test_1(self):
        with raises(ValueError) as e:
            create_dim_query("", "", "")
        assert str(e.value) == "table_name must not be null"

    @mark.it("generates correct SQL query to drop and replace")
    @mark.parametrize(
        "table_name",
        [
            "dim_counterparty",
            "dim_currency",
            "dim_design",
            "dim_location",
            "dim_payment_type",
            "dim_staff",
            "dim_transaction",
            "dim_date",
        ],
    )
    def test_2(self, table_name):
        with patch.dict(environ, PATCHDICT, clear=True):
            with patch(f"{PATCHPATH}.get_df_from_s3_parquet") as get_df_mock:
                with open(parquet_filepath(table_name), "rb") as f:
                    get_df_mock.return_value = read_parquet(f)
                result = create_dim_query(table_name, "", "")
        with open(sql_filepath(table_name), "r") as f:
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


@mark.context("testing generate_insert_into_statement")
class TestGenerateInsertIntoStatement:
    @mark.it("produces correct INSERT INTO statement for rows in dim_staff")
    def test_1(self):
        test_table_name = "dim_staff"
        test_df = read_parquet(parquet_filepath(test_table_name))
        test_df_columns = test_df.columns.values.tolist()
        result = generate_insert_into_statement(
            test_table_name, test_df_columns, test_df
        )
        with open(sql_filepath(f"insert_into_{test_table_name}"), "r") as f:
            expected = f.read()
        assert result == expected

    @mark.it("produces correct INSERT INTO statement for rows in dim_counterparty")
    def test_2(self):
        test_table_name = "dim_counterparty"
        test_df = read_parquet(parquet_filepath(test_table_name))
        test_df_columns = test_df.columns.values.tolist()
        result = generate_insert_into_statement(
            test_table_name, test_df_columns, test_df
        )
        with open(sql_filepath(f"insert_into_{test_table_name}"), "r") as f:
            expected = f.read()
        assert result == expected
