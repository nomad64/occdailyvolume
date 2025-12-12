"""
Tests for common/sqlite.py
"""
import sys
import os
import tempfile
from pathlib import Path
import pytest
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import sqlite


def test_validate_table_name_valid():
    """Test that valid table names pass validation"""
    # Should not raise exception
    sqlite._validate_table_name("valid_table")
    sqlite._validate_table_name("table123")
    sqlite._validate_table_name("Table_Name_123")
    sqlite._validate_table_name("volHist")


def test_validate_table_name_invalid_sql_injection():
    """Test that SQL injection attempts are rejected"""
    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table; DROP TABLE users;")

    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table' OR '1'='1")

    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table--")


def test_validate_table_name_invalid_special_chars():
    """Test that special characters are rejected"""
    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table-name")

    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table.name")

    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table name")

    with pytest.raises(ValueError, match="Invalid table name"):
        sqlite._validate_table_name("table@name")


def test_db_write_df_to_sql_with_context_manager():
    """Test that database connections are properly managed with context manager"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        test_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=3),
            'Value': [100, 200, 300]
        })
        test_df.set_index('Date', inplace=True)

        # Write to database
        sqlite.db_write_df_to_sql(db_path, "test_table", test_df)

        # Verify file was created
        assert Path(db_path).is_file()

        # Read back and verify
        result_df = sqlite.db_read_sql_to_df(db_path, "test_table")
        assert len(result_df) == 3
        assert 'Value' in result_df.columns


def test_db_write_df_to_sql_rejects_invalid_table_name():
    """Test that write function rejects invalid table names"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        test_df = pd.DataFrame({'A': [1, 2, 3]})

        with pytest.raises(ValueError, match="Invalid table name"):
            sqlite.db_write_df_to_sql(db_path, "bad; DROP TABLE users;", test_df)


def test_db_read_sql_to_df_rejects_invalid_table_name():
    """Test that read function rejects invalid table names"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")

        with pytest.raises(ValueError, match="Invalid table name"):
            sqlite.db_read_sql_to_df(db_path, "bad' OR '1'='1")


def test_db_read_sql_to_df_nonexistent_file():
    """Test that reading from non-existent file returns empty DataFrame"""
    result = sqlite.db_read_sql_to_df("/nonexistent/path/test.db", "test_table")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_db_read_sql_to_df_with_context_manager():
    """Test that read operations properly use context manager"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        test_df = pd.DataFrame({
            'Date': pd.date_range('2024-01-01', periods=5),
            'A': [1, 2, 3, 4, 5],
            'B': [10, 20, 30, 40, 50]
        })
        test_df.set_index('Date', inplace=True)

        # Write data
        sqlite.db_write_df_to_sql(db_path, "test_table", test_df)

        # Read data - should work even if we read multiple times (no connection leak)
        for _ in range(3):
            result_df = sqlite.db_read_sql_to_df(db_path, "test_table")
            assert len(result_df) == 5
            assert list(result_df.columns) == ['A', 'B']
