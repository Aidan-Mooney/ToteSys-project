from os import environ
from unittest.mock import patch
from pandas import read_parquet
from pytest import mark, raises

environ["DEV_ENVIRONMENT"] = "testing"
from src.utils.python.create_dim_query import create_dim_query

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
        WRITING = False  # when true, writes new SQL queries rather than tests equality against the SQL files. Set to False for normal testing
        if WRITING:
            with open(sql_filepath(table_name), "w") as f:
                f.write(result)
        else:
            with open(sql_filepath(table_name), "r") as f:
                expected = f.read()
            assert result == expected
