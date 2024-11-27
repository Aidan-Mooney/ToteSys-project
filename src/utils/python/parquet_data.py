from pandas import DataFrame
from io import BytesIO


def generate_parquet_of_dict(py_dict) -> bytes:
    """
    Return the parquet bytes of the passed-in python dictionary.

    Parameter:
    - py_dict: python dictionary to be converted to parquet.
    """
    df = DataFrame(py_dict)
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer.getvalue()
