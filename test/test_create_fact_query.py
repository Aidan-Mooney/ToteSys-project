from src.utils.python.create_fact_query import create_fact_query
from unittest.mock import patch, Mock
import pandas as pd

PATCH_PATH = 'src.utils.python.create_fact_query'
@patch.dict(f"{PATCH_PATH}.environ", {"transform_bucket_name": "test_bucket"}, clear=True)
def test_columns_can_be_used_in_sql_statement():
    
    age= ['5', '66', '11', '22', '145']
    first_name=['matt', 'matt2', 'aiden', 'anthony', 'chris']
    last_name=['a', 'b', 'c', 'd', 'e']
    column_names = ['age', 'first_name', 'last_name']
    stuff = list(zip(age, first_name, last_name))
    df = pd.DataFrame(stuff, columns=column_names)

    with patch(f'{PATCH_PATH}.get_df_from_s3_parquet') as df_mock:
        df_mock.return_value = df
        create_fact_query('test_table_name', '', 's3_client')