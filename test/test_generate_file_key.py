"""test the generate_file_key function"""

from datetime import datetime, timezone
from src.utils.python.generate_file_key import generate_file_key


class TestGenerateFileKey:
    """Testing top level"""

    def test_generate_file_key_returns_string(self):
        """check we return a string"""
        assert isinstance(generate_file_key("test", datetime.now(timezone.utc)), str)

    def test_generate_file_key_returns_string_starting_with_table_name(self):
        """check that we return 'tablename/' as start of string"""
        result = generate_file_key("test_table_name", datetime.now(timezone.utc))
        assert result[0:16] == "test_table_name/"

    def test_generate_file_key_returns_string_with_datetime_in_correct_format(
        self,
    ):
        """check that string has date in correct format"""
        date_to_test = datetime(2024, 11, 12, 15, 42, 42, 999999)
        result = generate_file_key("date_test_name", date_to_test)
        assert result == "date_test_name/2024/11/12/154242999999.parquet"

    def test_generate_file_key_returns_dot_parquet_extension_by_default(self):
        """test that .parquet is appended by default"""
        date_to_test = datetime(2024, 11, 12, 15, 42, 42, 123456)
        result = generate_file_key("date_test_name", date_to_test)
        assert result == "date_test_name/2024/11/12/154242123456.parquet"

    def test_generate_file_key_returns_given_extension_(self):
        """test that .parquet is appended by default"""
        date_to_test = datetime(2024, 11, 12, 15, 42, 42, 746352)
        result = generate_file_key("date_test_name", date_to_test, "json")
        assert result == "date_test_name/2024/11/12/154242746352.json"
