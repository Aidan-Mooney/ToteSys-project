from numpy import float64, int64, isnan
from pandas import DataFrame
from pytest import fixture, mark

from src.transform_utils.warehouse import Warehouse


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

        design_df = warehouse_df.dim_design.columns.values

        assert len(design_df) == 4
        assert design_df[0] == 'design_id'
        assert design_df[1] == 'design_name'
        assert design_df[2] == 'file_location'
        assert design_df[3] == 'file_name'

    @mark.it("checks that dim design dataframe contents returns correct value types")
    def test_4(self, warehouse_df):

        design_df = warehouse_df.dim_design

        assert isinstance(design_df.loc[1]['design_id'], int64)
        assert isinstance(design_df.loc[1]['design_name'], str)
        assert isinstance(design_df.loc[1]['file_location'], str)
        assert isinstance(design_df.loc[1]['file_name'], str)

    @mark.it("checks that dim design dataframe returns correct values within rows")
    def test_5(self, warehouse_df):

        design_df = warehouse_df.dim_design
        row = design_df.loc[1]

        assert row['design_id'] == 51
        assert row['design_name'] == 'Bronze'
        assert row['file_location'] == '/private'
        assert row['file_name'] == 'bronze-20221024-4dds.json'

class TestDimTransaction:
    @mark.it("checks that dim transaction returns data type dataframe")
    def test_6(self, warehouse_df):
        
        transaction_df = warehouse_df.dim_transaction

        assert isinstance(transaction_df, DataFrame)

    @mark.it("checks that dim transaction returns correct column names")
    def test_7(self, warehouse_df):

        transaction_df = warehouse_df.dim_transaction.columns.values

        assert len(transaction_df) == 4
        assert transaction_df[0] == 'transaction_id'
        assert transaction_df[1] == 'transaction_type'
        assert transaction_df[2] == 'sales_order_id'
        assert transaction_df[3] == 'purchase_order_id'

    @mark.it("checks that dim transaction dataframe contents returns correct value types")
    def test_8(self, warehouse_df):

        transaction_df = warehouse_df.dim_transaction

        assert isinstance(transaction_df.loc[1]['transaction_id'], int64)
        assert isinstance(transaction_df.loc[1]['transaction_type'], str)
        assert isinstance(transaction_df.loc[1]['sales_order_id'], float64)
        assert isinstance(transaction_df.loc[1]['purchase_order_id'], float64)

    @mark.it("checks that dim transaction dataframe returns correct values within rows")
    def test_9(self, warehouse_df):

        transaction_df = warehouse_df.dim_transaction
        row = transaction_df.loc[100]

        assert row['transaction_id'] == 101
        assert row['transaction_type'] == 'PURCHASE'
        assert isnan(row['sales_order_id'])
        assert row['purchase_order_id'] == 62.0

class TestDimCounterparty:
    @mark.it("checks that dim_counterparty returns data type dataframe")
    def test_10(self, warehouse_df):
        
        counterparty_df = warehouse_df.dim_transaction

        assert isinstance(counterparty_df, DataFrame)

    @mark.it("checks that dim counterparty returns correct column names")
    def test_11(self, warehouse_df):

        counterparty_df = warehouse_df.dim_counterparty.columns.values

        assert len(counterparty_df) == 9
        assert counterparty_df[0] == 'counterparty_legal_address_line_1'
        assert counterparty_df[1] == 'counterparty_legal_address_line_2'
        assert counterparty_df[2] == 'counterparty_legal_district'
        assert counterparty_df[3] == 'counterparty_legal_city'
        assert counterparty_df[4] == 'counterparty_legal_postal_code'
        assert counterparty_df[5] == 'counterparty_legal_country'
        assert counterparty_df[6] == 'counterparty_legal_phone_number'
        assert counterparty_df[7] == 'counterparty_id'
        assert counterparty_df[8] == 'counterparty_legal_name'