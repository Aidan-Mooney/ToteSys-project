from io import BytesIO
from pandas import DataFrame


def generate_parquet_of_df(df: DataFrame) -> bytes:
    """
    Return the parquet bytes of the passed-in dataframe.

    :param df: pandas DataFrame

    :returns parquet bytes of df:
    """
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer.getvalue()
