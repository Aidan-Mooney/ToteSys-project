from src.transform_utils.warehouse import Warehouse
from pytest import mark, fixture
from pandas import DataFrame

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