from src.utils.query_db import query_db
from pytest import mark, fixture
from unittest.mock import Mock


@fixture
def connect_to_db_mock():
    mock = Mock()
    call_mock = Mock()
    mock.return_value = call_mock
    call_mock.columns = [
        {"name": col_name}
        for col_name in [
            "id",
            "title",
            "ten_divided_by_2",
            "rating",
            "certificate",
            "avg_rating",
        ]
    ]
    call_mock.run.return_value = [
        [17, "Back to the Future", 5, 10, "U", "2.38"],
        [
            15,
            "Episode IV - A New Hope",
            5,
            10,
            "12",
            "2.14",
        ],
        [12, "Girl, Interrupted", 5, 8, "12", "1.90"],
        [
            20,
            "Raiders of the Lost Ark",
            5,
            9,
            "12",
            "2.14",
        ],
        [
            5,
            "The Care Bears Movie",
            5,
            10,
            None,
            "0.95",
        ],
        [
            13,
            "The Fellowship of the Ring",
            5,
            9,
            "12",
            "2.38",
        ],
        [19, "The Godfather", 5, 10, "18", "1.43"],
        [22, "Toy Story", 5, 10, "U", "1.43"],
    ]
    return mock


@fixture
def single_line_connect_to_db_mock():
    mock = Mock()
    call_mock = Mock()
    mock.return_value = call_mock
    call_mock.columns = [
        {"name": col_name}
        for col_name in [
            "id",
            "title",
            "ten_divided_by_2",
            "rating",
            "certificate",
            "avg_rating",
        ]
    ]
    call_mock.run.return_value = [[17, "Back to the Future", 5, 10, "U", "2.38"]]
    return mock


@fixture
def close_db_connection_mock():
    return Mock()


@mark.it("connect_to_db is called once")
def test_1(connect_to_db_mock, close_db_connection_mock):
    query_db("", connect_to_db_mock, close_db_connection_mock)
    connect_to_db_mock.assert_called_once


@mark.it("close_db_connection is called once")
def test_2(connect_to_db_mock, close_db_connection_mock):
    query_db("", connect_to_db_mock, close_db_connection_mock)
    close_db_connection_mock.assert_called_once


@mark.it("connect_to_db().run() is called once")
def test_3(connect_to_db_mock, close_db_connection_mock):
    query_db("", connect_to_db_mock, close_db_connection_mock)
    connect_to_db_mock.run.assert_called_once


@mark.it("connect_to_db().columns is called once")
def test_4(connect_to_db_mock, close_db_connection_mock):
    query_db("", connect_to_db_mock, close_db_connection_mock)
    connect_to_db_mock.columns.assert_called_once


@mark.it("correctly formats single row query without dict_name")
def test_5(single_line_connect_to_db_mock, close_db_connection_mock):
    result = query_db("", single_line_connect_to_db_mock, close_db_connection_mock, "")
    expected = {
        "id": 17,
        "title": "Back to the Future",
        "ten_divided_by_2": 5,
        "rating": 10,
        "certificate": "U",
        "avg_rating": "2.38",
    }
    assert result == expected


@mark.it("correctly formats single row query with dict_name")
def test_6(single_line_connect_to_db_mock, close_db_connection_mock):
    result = query_db(
        "", single_line_connect_to_db_mock, close_db_connection_mock, "test_name"
    )
    expected = {
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
    assert result == expected


@mark.it("correctly formats multi row query with dict_name")
def test_7(single_line_connect_to_db_mock, close_db_connection_mock):
    result = query_db("", single_line_connect_to_db_mock, close_db_connection_mock)
    expected = {
        "response": [
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
    assert result == expected


@mark.it("When kwargs are passed in, connect_to_db().run is called with kwargs")
def test_8(connect_to_db_mock, close_db_connection_mock):
    test_sql_string = "SELECT * ;"
    kwargs = {"test_kwarg": "test", "other_test": "test2"}
    query_db(test_sql_string, connect_to_db_mock, close_db_connection_mock, **kwargs)
    connect_to_db_mock.return_value.run.assert_called_with(test_sql_string, **kwargs)
