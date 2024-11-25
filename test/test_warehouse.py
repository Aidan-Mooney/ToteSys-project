from numpy import float64, int64, isnan
from pandas import DataFrame, read_parquet
from pytest import fixture, mark, raises
from os import listdir
from moto import mock_aws
from boto3 import client
from os import environ
from io import BytesIO
from logging import CRITICAL
from botocore.exceptions import ClientError

environ["DEV_ENVIRONMENT"] = "testing"
from src.utils.python.warehouse import Warehouse

TEST_BUCKET = "test_bucket"


@mock_aws
@fixture
def warehouse_df():
    s3_client = client("s3")
    warehouse = Warehouse([], "", s3_client)
    dir = "test/test_data/parquet_files"
    for filename in listdir(dir):
        table_name = filename[: -len(".parquet")]
        warehouse.dataframes[table_name] = read_parquet(f"{dir}/{filename}")
    return warehouse


@fixture(scope="function", autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    environ["AWS_ACCESS_KEY_ID"] = "testing"
    environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    environ["AWS_SECURITY_TOKEN"] = "testing"
    environ["AWS_SESSION_TOKEN"] = "testing"
    environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@mark.context("__init__")
class TestConstructor:
    @mark.it("checks that constructor returns a dictionary")
    def test_1(self, warehouse_df):
        assert isinstance(warehouse_df.dataframes, dict)

    @mock_aws
    @mark.it("retreives a dataframe from s3 bucket")
    def test_2(self):
        s3_client = client("s3")
        s3_client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_df = read_parquet("test/test_data/parquet_files/transaction.parquet")
        parquet_data = BytesIO()
        test_df.to_parquet(parquet_data, index=False)
        test_key = "transaction/dddddd.parquet"
        s3_client.put_object(
            Bucket=TEST_BUCKET, Key=test_key, Body=parquet_data.getvalue()
        )
        warehouse = Warehouse([test_key], TEST_BUCKET, s3_client)
        transaction = warehouse.dim_transaction
        assert isinstance(transaction, DataFrame)
        cols = transaction.columns.values.tolist()
        assert cols == [
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id",
        ]

    @mock_aws
    @mark.it("Makes a critical log when the bucket doesn't exist")
    def test_3(self, caplog):
        s3_client = client("s3")
        test_key = "transaction/dddddd.parquet"
        caplog.set_level(CRITICAL)
        with raises(ClientError):
            Warehouse([test_key], TEST_BUCKET, s3_client)
        assert "CRITICAL" in caplog.text
        assert "encountered error retrieving file" in caplog.text


@mark.context("dim_design")
class TestDimDesign:
    @mark.it("checks that dim design returns data type dataframe")
    def test_2(self, warehouse_df):
        design_df = warehouse_df.dim_design
        assert isinstance(design_df, DataFrame)

    @mark.it("checks that dim design returns correct column names")
    def test_3(self, warehouse_df):
        design_df = warehouse_df.dim_design.columns.values
        assert len(design_df) == 4
        assert design_df[0] == "design_id"
        assert design_df[1] == "design_name"
        assert design_df[2] == "file_location"
        assert design_df[3] == "file_name"

    @mark.it("checks that dim design dataframe contents returns correct value types")
    def test_4(self, warehouse_df):
        design_df = warehouse_df.dim_design

        assert isinstance(design_df.loc[1]["design_id"], int64)
        assert isinstance(design_df.loc[1]["design_name"], str)
        assert isinstance(design_df.loc[1]["file_location"], str)
        assert isinstance(design_df.loc[1]["file_name"], str)

    @mark.it("checks that dim design dataframe returns correct values within rows")
    def test_5(self, warehouse_df):
        design_df = warehouse_df.dim_design
        row = design_df.loc[1]

        assert row["design_id"] == 51
        assert row["design_name"] == "Bronze"
        assert row["file_location"] == "/private"
        assert row["file_name"] == "bronze-20221024-4dds.json"


@mark.context("dim_transaction")
class TestDimTransaction:
    @mark.it("checks that dim transaction returns data type dataframe")
    def test_6(self, warehouse_df):
        transaction_df = warehouse_df.dim_transaction

        assert isinstance(transaction_df, DataFrame)

    @mark.it("checks that dim transaction returns correct column names")
    def test_7(self, warehouse_df):
        transaction_df = warehouse_df.dim_transaction.columns.values

        assert len(transaction_df) == 4
        assert transaction_df[0] == "transaction_id"
        assert transaction_df[1] == "transaction_type"
        assert transaction_df[2] == "sales_order_id"
        assert transaction_df[3] == "purchase_order_id"

    @mark.it(
        "checks that dim transaction dataframe contents returns correct value types"
    )
    def test_8(self, warehouse_df):
        transaction_df = warehouse_df.dim_transaction

        assert isinstance(transaction_df.loc[1]["transaction_id"], int64)
        assert isinstance(transaction_df.loc[1]["transaction_type"], str)
        assert isinstance(transaction_df.loc[1]["sales_order_id"], float64)
        assert isinstance(transaction_df.loc[1]["purchase_order_id"], float64)

    @mark.it("checks that dim transaction dataframe returns correct values within rows")
    def test_9(self, warehouse_df):
        transaction_df = warehouse_df.dim_transaction
        row = transaction_df.loc[100]

        assert row["transaction_id"] == 101
        assert row["transaction_type"] == "PURCHASE"
        assert isnan(row["sales_order_id"])
        assert row["purchase_order_id"] == 62.0


@mark.context("dim_counterparty")
class TestDimCounterparty:
    @mark.it("checks that dim_counterparty returns data type dataframe")
    def test_10(self, warehouse_df):
        counterparty_df = warehouse_df.dim_counterparty

        assert isinstance(counterparty_df, DataFrame)

    @mark.it("checks that dim counterparty returns correct column names")
    def test_11(self, warehouse_df):
        counterparty_df = warehouse_df.dim_counterparty.columns.values

        assert len(counterparty_df) == 9
        assert counterparty_df[0] == "counterparty_legal_address_line_1"
        assert counterparty_df[1] == "counterparty_legal_address_line_2"
        assert counterparty_df[2] == "counterparty_legal_district"
        assert counterparty_df[3] == "counterparty_legal_city"
        assert counterparty_df[4] == "counterparty_legal_postal_code"
        assert counterparty_df[5] == "counterparty_legal_country"
        assert counterparty_df[6] == "counterparty_legal_phone_number"
        assert counterparty_df[7] == "counterparty_id"
        assert counterparty_df[8] == "counterparty_legal_name"

    @mark.it("checks that dim counterparty returns correct data types")
    def test_12(self, warehouse_df):
        counterparty_df = warehouse_df.dim_counterparty

        assert isinstance(
            counterparty_df.loc[3]["counterparty_legal_address_line_1"], str
        )
        assert isinstance(
            counterparty_df.loc[3]["counterparty_legal_address_line_2"], str | None
        )
        assert isinstance(
            counterparty_df.loc[3]["counterparty_legal_district"], str | None
        )
        assert isinstance(counterparty_df.loc[3]["counterparty_legal_city"], str)
        assert isinstance(counterparty_df.loc[3]["counterparty_legal_postal_code"], str)
        assert isinstance(counterparty_df.loc[3]["counterparty_legal_country"], str)
        assert isinstance(
            counterparty_df.loc[3]["counterparty_legal_phone_number"], str
        )
        assert isinstance(counterparty_df.loc[3]["counterparty_id"], int64)
        assert isinstance(counterparty_df.loc[3]["counterparty_legal_name"], str)


@mark.context("dim_currency")
class TestDimCurrency:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        df = warehouse_df.dim_currency
        assert isinstance(df, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.dim_currency.columns.values.tolist()
        assert cols == ["currency_id", "currency_code", "currency_name"]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.dim_currency
        col_dtypes = {"currency_id": int64, "currency_code": str, "currency_name": str}
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])


@mark.context("dim_payment_type")
class TestDimPaymentType:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        df = warehouse_df.dim_payment_type
        assert isinstance(df, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.dim_payment_type.columns.values.tolist()
        assert cols == ["payment_type_id", "payment_type_name"]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.dim_payment_type
        col_dtypes = {"payment_type_id": int64, "payment_type_name": str}
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])


@mark.context("dim_location")
class TestDimLocation:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        df = warehouse_df.dim_location
        assert isinstance(df, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.dim_location.columns.values.tolist()
        assert cols == [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.dim_location
        col_dtypes = {
            "location_id": int64,
            "address_line_1": str,
            "city": str,
            "postal_code": str,
            "country": str,
            "phone": str,
        }
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])


@mark.context("dim_staff")
class TestDimStaff:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        df = warehouse_df.dim_staff
        assert isinstance(df, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.dim_staff.columns.values.tolist()
        assert cols == [
            "staff_id",
            "first_name",
            "last_name",
            "email_address",
            "department_name",
            "location",
        ]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.dim_staff
        col_dtypes = {
            "staff_id": int64,
            "first_name": str,
            "last_name": str,
            "email_address": str,
            "department_name": str,
            "location": str,
        }
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])


@mark.context("fact_sales_order")
class TestFactSalesOrder:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        df = warehouse_df.fact_sales_order
        assert isinstance(df, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.fact_sales_order.columns.values.tolist()
        assert cols == [
            "sales_order_id",
            "design_id",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
        ]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.fact_sales_order
        col_dtypes = {
            "sales_order_id": int64,
            "design_id": int64,
            "sales_staff_id": int64,
            "counterparty_id": int64,
            "units_sold": int64,
            "unit_price": float64,
            "currency_id": int64,
            "agreed_delivery_date": str,
            "agreed_payment_date": str,
            "agreed_delivery_location_id": int64,
            "created_date": str,
            "created_time": str,
            "last_updated_date": str,
            "last_updated_time": str,
        }
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])


@mark.context("fact_payment")
class TestFactPayment:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        fact_payment = warehouse_df.fact_payment
        assert isinstance(fact_payment, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.fact_payment.columns.values.tolist()
        assert cols == [
            "payment_id",
            "transaction_id",
            "counterparty_id",
            "payment_amount",
            "currency_id",
            "payment_type_id",
            "paid",
            "payment_date",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
        ]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.fact_payment
        col_dtypes = {
            "payment_id": int64,
            "transaction_id": int64,
            "counterparty_id": int64,
            "payment_amount": float64,
            "currency_id": int64,
            "payment_type_id": int64,
            "payment_date": str,
            "created_date": str,
            "created_time": str,
            "last_updated_date": str,
            "last_updated_time": str,
        }
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])


@mark.context("fact_purchase_order")
class TestFactPurchaseOrder:
    @mark.it("returns data type dataframe")
    def test_1(self, warehouse_df):
        fact_purchase_order = warehouse_df.fact_purchase_order
        assert isinstance(fact_purchase_order, DataFrame)

    @mark.it("has the correct column headers")
    def test_2(self, warehouse_df):
        cols = warehouse_df.fact_purchase_order.columns.values.tolist()
        assert cols == [
            "purchase_order_id",
            "staff_id",
            "counterparty_id",
            "item_code",
            "item_quantity",
            "item_unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
        ]

    @mark.it("has columns containing the correct data types")
    def test_3(self, warehouse_df):
        df = warehouse_df.fact_purchase_order
        col_dtypes = {
            "purchase_order_id": int64,
            "staff_id": int64,
            "counterparty_id": int64,
            "item_code": str,
            "item_quantity": int64,
            "item_unit_price": float64,
            "currency_id": int64,
            "agreed_delivery_date": str,
            "agreed_payment_date": str,
            "agreed_delivery_location_id": int64,
            "created_date": str,
            "created_time": str,
            "last_updated_date": str,
            "last_updated_time": str,
        }
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])
