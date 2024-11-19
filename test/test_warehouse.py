from src.transform_utils.warehouse import Warehouse
from pytest import mark, fixture
from pandas import DataFrame
from numpy import int64

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