from moto import mock_aws
from boto3 import client
from pandas import read_parquet, DataFrame
from os import environ
from io import BytesIO
from unittest.mock import patch
from datetime import datetime
from pytest import mark
from botocore.exceptions import ClientError
from logging import CRITICAL

environ["DEV_ENVIRONMENT"] = "testing"

from src.lambdas.transform import lambda_handler as transform
from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet

ig_bucket_name = "ig_bucket"
tf_bucket_name = "tf_bucket"
PATCHED_ENVIRON = {
    "ingest_bucket_name": ig_bucket_name,
    "transform_bucket_name": tf_bucket_name,
    "static_address_path": "static/address.parquet",
    "static_department_path": "static/department.parquet",
}


@mock_aws
def put_parquet_file_to_s3(
    filepath, bucket_name, s3_client, table_name, static_path=False
):
    test_df = read_parquet(filepath)
    parquet_data = BytesIO()
    test_df.to_parquet(parquet_data, index=False)
    if static_path:
        s3_key = f"static/{table_name}.parquet"
    else:
        s3_key = f"{table_name}/yadayada.parquet"
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_data.getvalue())


@mock_aws
@mark.it(
    "loads a single table into the warehouse, then its transformed counterpart is added to the transform bucket"
)
def test_1():
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
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            transform({"transaction": test_key})
    result_key = f"dim_transaction/{test_time[0]}/{test_time[1]}/{test_time[2]}/{test_time[3]}{test_time[4]}00000000.parquet"
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, result_key)
    assert isinstance(result, DataFrame)
    assert result.columns.values.tolist() == [
        "transaction_id",
        "transaction_type",
        "sales_order_id",
        "purchase_order_id",
    ]


@mark.it(
    "constructs the corresponding data warehouse tables for the ingest tables given in the event"
)
@mock_aws
def test_2():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    for table_name in ["address", "counterparty", "currency", "design", "department"]:
        put_parquet_file_to_s3(
            f"test/test_data/parquet_files/{table_name}.parquet",
            ig_bucket_name,
            s3_client,
            table_name,
        )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            transform(
                {
                    "address": "address/yadayada.parquet",
                    "counterparty": "counterparty/yadayada.parquet",
                    "currency": "currency/yadayada.parquet",
                    "design": "design/yadayada.parquet",
                }
            )
    tf_parquet_list = [
        item["Key"]
        for item in s3_client.list_objects_v2(Bucket=tf_bucket_name)["Contents"]
    ]
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, tf_parquet_list[0])
    assert result.columns.values.tolist() == [
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
        "counterparty_id",
        "counterparty_legal_name",
    ]


@mock_aws
@mark.it(
    "successfully constructs staff table without department table present in the event and doesn't add tables to the transform bucket which weren't present in the event"
)
def test_4():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/staff.parquet", ig_bucket_name, s3_client, "staff"
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/department.parquet",
        ig_bucket_name,
        s3_client,
        "department",
        True,
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            transform(
                {
                    "staff": "staff/yadayada.parquet",
                }
            )
    result_key = f"dim_staff/{test_time[0]}/{test_time[1]}/{test_time[2]}/{test_time[3]}{test_time[4]}00000000.parquet"
    assert s3_client.list_objects_v2(Bucket=tf_bucket_name)["KeyCount"] == 1
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, result_key)
    assert result.columns.values.tolist() == [
        "staff_id",
        "first_name",
        "last_name",
        "email_address",
        "department_name",
        "location",
    ]


@mock_aws
@mark.it(
    "successfully constructs counterparty table without address table present in the event"
)
def test_5():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/counterparty.parquet",
        ig_bucket_name,
        s3_client,
        "counterparty",
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/address.parquet",
        ig_bucket_name,
        s3_client,
        "address",
        True,
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            transform(
                {
                    "counterparty": "counterparty/yadayada.parquet",
                }
            )
    result_key = f"dim_counterparty/{test_time[0]}/{test_time[1]}/{test_time[2]}/{test_time[3]}{test_time[4]}00000000.parquet"
    assert s3_client.list_objects_v2(Bucket=tf_bucket_name)["KeyCount"] == 1
    result = get_df_from_s3_parquet(s3_client, tf_bucket_name, result_key)
    assert result.columns.values.tolist() == [
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
        "counterparty_id",
        "counterparty_legal_name",
    ]
    assert s3_client.list_objects_v2(Bucket=tf_bucket_name)["KeyCount"] == 1


@mock_aws
@mark.it(
    "doesn't construct location and department tables when nothing is passed in the event"
)
def test_3():
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/address.parquet",
        ig_bucket_name,
        s3_client,
        "std_address",
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/department.parquet",
        ig_bucket_name,
        s3_client,
        "std_department",
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        transform({})
    assert not s3_client.list_objects_v2(Bucket=tf_bucket_name)["KeyCount"]


@mock_aws
@mark.it("Raises critical log when fails to write to s3")
def test_6(caplog):
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/counterparty.parquet",
        ig_bucket_name,
        s3_client,
        "counterparty",
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/address.parquet",
        ig_bucket_name,
        s3_client,
        "address",
        True,
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.datetime") as mock:
            test_time = [2024, 11, 20, 21, 48]
            mock.now.return_value = datetime(*test_time)
            with patch(
                "src.lambdas.transform.write_to_s3",
                side_effect=ClientError(
                    {
                        "Error": {
                            "Code": "InternalServiceError",
                            "Message": "not our problem",
                        }
                    },
                    "testing",
                ),
            ):
                caplog.set_level(CRITICAL)
                transform(
                    {
                        "counterparty": "counterparty/yadayada.parquet",
                    }
                )
    assert "CRITICAL" in caplog.text
    assert "failed to write to s3" in caplog.text


@mock_aws
@mark.it("raises critical log when fails to retrieve attribute from warehouse")
def test_7(caplog):
    s3_client = client("s3")
    s3_client.create_bucket(
        Bucket=ig_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket=tf_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/counterparty.parquet",
        ig_bucket_name,
        s3_client,
        "counterparty",
    )
    put_parquet_file_to_s3(
        "test/test_data/parquet_files/address.parquet",
        ig_bucket_name,
        s3_client,
        "address",
        True,
    )
    with patch.dict(
        environ,
        PATCHED_ENVIRON,
        clear=True,
    ):
        with patch("src.lambdas.transform.getattr") as mock:
            mock.side_effect = AttributeError
            caplog.set_level(CRITICAL)
            transform(
                {
                    "counterparty": "counterparty/yadayada.parquet",
                }
            )
    assert "CRITICAL" in caplog.text
    assert "Unable to access attribute" in caplog.text
    # print(f"this guy👉{caplog.text}")
    # print("helo")
