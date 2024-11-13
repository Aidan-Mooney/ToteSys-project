import pandas as pd
from io import BytesIO

def parquet_data(py_dict):
    
    df = pd.DataFrame(py_dict)

    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)

    return out_buffer.getvalue()

