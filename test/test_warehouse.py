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

    @mark.it("checks that dim design returns correct column names")
    def test_3(self, warehouse_df):
        design_df = warehouse_df.dim_design.columns

        print(design_df)
        assert design_df
        pass


class TestFactPurchaseOrder:
    @mark.it("returns data type dataframe")
    def test_2(self, warehouse_df):
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
        fact_purchase_order = warehouse_df.fact_purchase_order
        assert isinstance(fact_purchase_order.loc[1]["purchase_order_id"], int64)
        assert isinstance(fact_purchase_order.loc[1]["staff_id"], int64)
        assert isinstance(fact_purchase_order.loc[1]["counterparty_id"], int64)
        assert isinstance(fact_purchase_order.loc[1]["item_code"], str)
        assert isinstance(fact_purchase_order.loc[1]["item_quantity"], int64)
        assert isinstance(fact_purchase_order.loc[1]["item_unit_price"], float64)
        assert isinstance(fact_purchase_order.loc[1]["currency_id"], int64)
        assert isinstance(fact_purchase_order.loc[1]["agreed_delivery_date"], str)
        assert isinstance(fact_purchase_order.loc[1]["agreed_payment_date"], str)
        assert isinstance(
            fact_purchase_order.loc[1]["agreed_delivery_location_id"], int64
        )
        assert isinstance(fact_purchase_order.loc[1]["created_date"], date)
        assert isinstance(fact_purchase_order.loc[1]["created_time"], time)
        assert isinstance(fact_purchase_order.loc[1]["last_updated_date"], date)
        assert isinstance(fact_purchase_order.loc[1]["last_updated_time"], time)
