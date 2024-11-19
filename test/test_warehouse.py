from src.transform_utils.warehouse import Warehouse
from pytest import mark, fixture

@fixture(autouse=True)
def warehouse_df():
    return Warehouse("test/test_data/parquet_files").dataframes


class TestConstructor:
    @mark.it("checks that constructor returns a dictionary")
    def test_1(self, warehouse_df):
        
        assert isinstance(warehouse_df, dict)

class TestDimDesign:
    @mark.it("checks that dim design column names are correct")
    def test_2(self, warehouse_df):
        design_cols = warehouse_df.dim_design().columns
        print(design_cols)
        