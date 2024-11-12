from src.utils.parquet_data import parquet_data

def test_valid_dictionary_creates_parquet_file():

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

    assert output == 'file created'