from io import BytesIO
from pandas import DataFrame


def generate_parquet_of_df(df: DataFrame):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer.getvalue()
