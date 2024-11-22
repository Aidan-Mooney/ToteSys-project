from os import environ
from pytest import raises, mark
from unittest.mock import patch

environ["DEV_ENVIRONMENT"] = "testing"

from src.utils.python.generate_warehouse_query import (
    generate_warehouse_query,
    InvalidTableNameError,
)


PATCHPATH = "src.utils.python.generate_warehouse_query"


@mark.it("raises an error if an invalid table name is passed in")
def test_1():
    test_table_name = "i_dont_start_with_dim_or_fact"
    test_path = "iamirrelavent.parquet"
    with raises(InvalidTableNameError) as e:
        generate_warehouse_query(test_table_name, test_path)
    assert str(e.value) == f"This table has an invalid name: '{test_table_name}'"


@mark.it("calls create fact query when the table name starts with fact")
def test_2():
    test_table_name = "fact_suffix"
    test_path = "iamirrelavent.parquet"
    with patch(f"{PATCHPATH}.create_fact_query") as fact_func_mock:
        return_val = "test_return"
        fact_func_mock.return_value = return_val
        generate_warehouse_query(test_table_name, test_path)
    fact_func_mock.assert_called_once_with(test_table_name, test_path)


@mark.it("returns the correct fact query string")
def test_3():
    test_table_name = "fact_suffix"
    test_path = "iamirrelavent.parquet"
    with patch(f"{PATCHPATH}.create_fact_query") as fact_func_mock:
        return_val = "test_return"
        fact_func_mock.return_value = return_val
        result = generate_warehouse_query(test_table_name, test_path)
    assert result == return_val


@mark.it("calls create dim query when the table name starts with dim")
def test_4():
    test_table_name = "dim_suffix"
    test_path = "iamirrelavent.parquet"
    with patch(f"{PATCHPATH}.create_dim_query") as dim_func_mock:
        return_val = "test_return"
        dim_func_mock.return_value = return_val
        generate_warehouse_query(test_table_name, test_path)
    dim_func_mock.assert_called_once_with(test_table_name, test_path)


@mark.it("returns the correct dim query string")
def test_5():
    test_table_name = "dim_suffix"
    test_path = "iamirrelavent.parquet"
    with patch(f"{PATCHPATH}.create_dim_query") as dim_func_mock:
        return_val = "test_return"
        dim_func_mock.return_value = return_val
        result = generate_warehouse_query(test_table_name, test_path)
    assert result == return_val
