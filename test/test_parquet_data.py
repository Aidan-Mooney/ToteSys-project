from src.utils.parquet_data import parquet_data

def test_output_is_in_parquet_bytes():

    input_dict = {
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
    
    output = parquet_data(input_dict)
    print(output)

    assert output == 