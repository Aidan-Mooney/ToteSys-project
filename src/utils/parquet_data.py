import pandas as pd
# import pyarrow

def parquet_data(py_dict):
    
    df = pd.DataFrame(py_dict['test_name'])
    print(df)
    df.to_parquet('example.parquet', engine="pyarrow")

    parq_read = pd.read_parquet('example.parquet', engine='pyarrow')
    print(parq_read)
    return "file created"




data = {
        "test_name": [
            {
                "id": 17,
                "title": "Back to the Future",
                "ten_divided_by_2": 5,
                "rating": 10,
                "certificate": "U",
                "avg_rating": "2.38",
            }
        ]
    }

print(parquet_data(data))