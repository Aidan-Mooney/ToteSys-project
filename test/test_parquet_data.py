from src.utils.python.parquet_data import generate_parquet_of_dict as parquet_data
from io import BytesIO
import pandas as pd
import json


def test_output_is_stored_in_bytes():
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

    assert isinstance(output, bytes)


def test_output_can_be_read_from_parquet_format():
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
    df = pd.read_parquet(BytesIO(output))

    json_string = df.to_json()
    output_dict = json.loads(json_string)
    title = list(output_dict)[0]
    final_output = {title: [value for _, value in output_dict[title].items()]}
    assert final_output == input_dict


def test_function_can_accept_and_process_larger_datasets():
    input_dict = {
        "test_name": [
            {
                "id": 17,
                "title": "Back to the Future",
                "ten_divided_by_2": 5,
                "rating": 10,
                "certificate": "U",
                "avg_rating": "2.38",
            },
            {
                "id": 18,
                "title": "Back to the Future",
                "ten_divided_by_2": 5,
                "rating": 10,
                "certificate": "U",
                "avg_rating": "2.38",
            },
            {
                "id": 19,
                "title": "Back to the Future",
                "ten_divided_by_2": 5,
                "rating": 10,
                "certificate": "U",
                "avg_rating": "2.38",
            },
            {
                "id": 20,
                "title": "Back to the Future",
                "ten_divided_by_2": 5,
                "rating": 10,
                "certificate": "U",
                "avg_rating": "2.38",
            },
            {
                "id": 21,
                "title": "Back to the Future",
                "ten_divided_by_2": 5,
                "rating": 10,
                "certificate": "U",
                "avg_rating": "2.38",
            },
        ]
    }

    output = parquet_data(input_dict)
    df = pd.read_parquet(BytesIO(output))

    json_string = df.to_json()
    output_dict = json.loads(json_string)
    title = list(output_dict)[0]
    final_output = {title: [value for _, value in output_dict[title].items()]}
    assert final_output == input_dict
