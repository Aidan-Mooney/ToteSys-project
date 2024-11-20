from src.lambdas.mattyc_in_tha_ware_house import transform
from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
from moto import mock_aws
from boto3 import client
from pandas import read_parquet, DataFrame
from os import environ
from io import BytesIO
from unittest.mock import patch
from datetime import datetime
@mock_aws
def test_1():
    ig_bucket_name = "ig_bucket"
    tf_bucket_name = "tf_bucket"
    s3_client = client("s3")
    s3_client.create_bucket(
                Bucket=ig_bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
    s3_client.create_bucket(
                Bucket=tf_bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
            )
    test_df = read_parquet("test/test_data/parquet_files/transaction.parquet")
    parquet_data = BytesIO()
    test_df.to_parquet(parquet_data, index=False)
    test_key = "transaction/dddddd.parquet"
    s3_client.put_object(
        Bucket=ig_bucket_name, Key=test_key, Body=parquet_data.getvalue()
    )
    test_time = [2024, 11, 20, 21, 48]
    with patch.dict("src.lambdas.mattyc_in_tha_ware_house.environ", {"ig_bucket_name":"ig_bucket", "tf_bucket_name":"tf_bucket"}, clear=True):
        with patch("src.lambdas.mattyc_in_tha_ware_house.datetime") as mock:
            mock.now.return_value = datetime(*test_time)
            transform({"transaction": test_key})
    result_key = f"transaction/{test_time[0]}/{test_time[1]}/{test_time[2]}/{test_time[3]}{test_time[4]}00000000.parquet"
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, result_key)
    assert isinstance(result, DataFrame)
    assert result.columns.values.tolist() == ["transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"]

def test_2():
