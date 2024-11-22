from os import environ
from unittest.mock import patch
from pandas import read_parquet

environ["DEV_ENVIRONMENT"] = "testing"
from src.utils.python.create_dim_query import create_dim_query

PATCHPATH = "src.utils.python.create_dim_query"
PATCHDICT = {"transform_bucket_name": "test"}


def test_1():
    with patch.dict(environ, PATCHDICT, clear=True):
        with patch(f"{PATCHPATH}.get_df_from_s3_parquet") as get_df_mock:
            with open("test/test_data/parquet_files/dim_staff.parquet", "rb") as f:
                get_df_mock.return_value = read_parquet(f)
            result = create_dim_query("staff", "", "")
    with open("output.txt", "w") as f:
        f.write(result)
