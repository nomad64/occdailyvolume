"""
Tests for common/updater.py
"""
from datetime import date
import pandas as pd
from unittest.mock import MagicMock, patch
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import updater

def test_backfill_db_to_previous_month_empty_db(mocker):
    """
    Test the backfill_db_to_previous_month function with an empty database
    """
    # Mock the sqlite functions
    mocker.patch('common.sqlite.db_read_sql_to_df', return_value=pd.DataFrame())
    mock_db_write = mocker.patch('common.sqlite.db_write_df_to_sql')

    # Mock the occ function
    mock_get_volume = mocker.patch('common.occ.get_volume_by_month_to_df', return_value=pd.DataFrame({'A': [1, 2, 3]}))

    # Call the function
    updater.backfill_db_to_previous_month("http://fake.url", "csv", "fake.db", "fake_table")

    # Assert that the functions were called
    mock_get_volume.assert_called()
    mock_db_write.assert_called()

def test_backfill_db_to_previous_month_up_to_date_db(mocker):
    """
    Test the backfill_db_to_previous_month function with an up-to-date database
    """
    # Mock the sqlite functions
    today = date.today()
    backfill_end_date = date(2008, 1, 1)
    mock_df = pd.DataFrame(index=pd.to_datetime(pd.date_range(start=backfill_end_date, end=today, freq='MS')))
    mocker.patch('common.sqlite.db_read_sql_to_df', return_value=mock_df)
    mock_db_write = mocker.patch('common.sqlite.db_write_df_to_sql')

    # Mock the occ function
    mock_get_volume = mocker.patch('common.occ.get_volume_by_month_to_df')

    # Call the function
    updater.backfill_db_to_previous_month("http://fake.url", "csv", "fake.db", "fake_table")

    # Assert that the functions were not called
    mock_get_volume.assert_not_called()
    mock_db_write.assert_not_called()
