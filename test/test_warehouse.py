from src.transform_utils.warehouse import Warehouse
from pytest import mark, fixture
from pandas import DataFrame
from numpy import float64, int64
from datetime import date, time


@fixture(autouse=True)
def warehouse_df():
    return Warehouse("test/test_data/parquet_files")


class TestConstructor:
    @mark.it("checks that constructor returns a dictionary")
    def test_1(self, warehouse_df):
        assert isinstance(warehouse_df.dataframes, dict)


class TestDimDesign:
    @mark.it("checks that dim design returns data type dataframe")
    def test_2(self, warehouse_df):
        design_df = warehouse_df.dim_design

        assert isinstance(design_df, DataFrame)


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
            "created_date": date,
            "created_time": time,
            "last_updated_date": date,
            "last_updated_time": time,
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
            "created_date": date,
            "created_time": time,
            "last_updated_date": date,
            "last_updated_time": time,
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
            "created_date": date,
            "created_time": time,
            "last_updated_date": date,
            "last_updated_time": time,
        }
        for col in col_dtypes:
            assert isinstance(df.loc[1][col], col_dtypes[col])
