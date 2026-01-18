"""
Additional tests to improve code coverage
"""
import sys
import os
import pandas as pd
import pytest
from datetime import date
from dateutil.relativedelta import relativedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import occ
from common import updater
from common import sqlite

# --- common.occ tests ---

def test_volume_csv_month_clean_sep_invalid_header():
    """Test that invalid header raises ValueError"""
    csv_data = "Garbage Data\nMore Garbage"
    with pytest.raises(ValueError, match="Could not parse Report Date from CSV"):
        occ.volume_csv_month_clean_sep(csv_data)

def test_volume_df_create_with_merge_df():
    """Test volume_df_create merging with existing dataframe"""
    # Create sample volume dict
    vol_dict = {
        "contracts": "Date,Col1\n2024-01-01,100",
    }
    
    # Create existing df to merge with
    existing_df = pd.DataFrame(
        {'Col1': [200]},
        index=pd.DatetimeIndex(['2024-01-02'], name='Date')
    )
    
    # Call function with merge_df
    result_df = occ.volume_df_create(vol_dict, merge_df=existing_df)
    
    # Verify merge happened (concatenation)
    assert len(result_df) == 2
    assert result_df.index.min() == pd.Timestamp('2024-01-01')
    assert result_df.index.max() == pd.Timestamp('2024-01-02')


# --- common.updater tests ---

def test_backfill_exceptions_in_loop():
    """Test exceptions in the backfill loop explicitly"""
    # We want to test ONLY the backfill loop exceptions.
    # To avoid triggering forward fill, we need db_max_date >= prev_month.
    # To trigger backfill loop, we need db_min_date > backfill_end_date.
    
    today = date.today()
    prev_month = today + relativedelta(day=1) - relativedelta(months=1)
    
    # DB range: 2008-04-01 to prev_month
    dates = [date(2008, 4, 1), prev_month]
    mock_db_df = pd.DataFrame(
        {'Data': [1, 2]},
        index=pd.DatetimeIndex(dates, name='Date')
    )
    
    with patch('common.sqlite.db_read_sql_to_df', return_value=mock_db_df), \
         patch('common.sqlite.db_write_df_to_sql'), \
         patch('common.occ.get_volume_by_month_to_df') as mock_get_vol:
             
        # Backfill loop:
        # min=2008-04-01.
        # working starts at 2008-03-01.
        # Loop continues while working > 2008-01-01.
        # Iteration 1: 2008-03-01 -> Call 1 (ValueError)
        # Iteration 2: 2008-02-01 -> Call 2 (ConnectionError)
        # Iteration 3: 2008-01-01 -> Stop loop.
        
        # We need 2 side effects.
        mock_get_vol.side_effect = [
            ValueError("Unavailable"),
            ConnectionError("Network fail")
        ]
        
        updater.backfill_db_to_previous_month("url", "csv", "db", "table")
        
        # Should have attempted 2 calls
        assert mock_get_vol.call_count == 2

def test_forward_fill_logic():
    """Test forward filling logic and exceptions"""
    # We want to test ONLY the forward fill loop exceptions.
    # To avoid triggering backfill loop, we need db_min_date <= backfill_end_date (2008-01-01).
    # To trigger forward fill, we need db_max_date < prev_month.
    
    today = date.today()
    prev_month = today + relativedelta(day=1) - relativedelta(months=1)
    
    # DB max date is 3 months ago (so we need to fill 3 months)
    db_max_date = prev_month - relativedelta(months=3)
    
    dates = [date(2008, 1, 1), db_max_date]
    mock_db_df = pd.DataFrame(
        {'Data': [1, 2]},
        index=pd.DatetimeIndex(dates, name='Date')
    )
    
    with patch('common.sqlite.db_read_sql_to_df', return_value=mock_db_df), \
         patch('common.sqlite.db_write_df_to_sql'), \
         patch('common.occ.get_volume_by_month_to_df') as mock_get_vol:
             
        # Forward fill loop:
        # max = 3 months ago.
        # working starts at max + 1 month.
        # Loop continues while working <= prev_month.
        # Iteration 1: max+1 -> Call 1 (ValueError)
        # Iteration 2: max+2 -> Call 2 (TimeoutError)
        # Iteration 3: max+3 (prev_month) -> Call 3 (Success)
        
        mock_get_vol.side_effect = [
            ValueError("Unavailable"),
            TimeoutError("Timeout"),
            pd.DataFrame()
        ]
        
        updater.backfill_db_to_previous_month("url", "csv", "db", "table")
        
        assert mock_get_vol.call_count == 3


def test_db_already_current():
    """Test when DB is already up to date"""
    # No backfill (min <= 2008-01-01)
    # No forward fill (max > prev_month) -- Wait, max cannot be > prev_month usually, but max >= prev_month works.
    
    today = date.today()
    prev_month = today + relativedelta(day=1) - relativedelta(months=1)
    
    dates = [date(2008, 1, 1), prev_month]
    mock_db_df = pd.DataFrame(
        {'Data': [1, 2]},
        index=pd.DatetimeIndex(dates, name='Date')
    )
    
    with patch('common.sqlite.db_read_sql_to_df', return_value=mock_db_df), \
         patch('common.occ.get_volume_by_month_to_df') as mock_get_vol:
             
        updater.backfill_db_to_previous_month("url", "csv", "db", "table")
        
        assert mock_get_vol.call_count == 0