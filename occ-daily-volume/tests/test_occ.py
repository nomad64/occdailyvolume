"""
Tests for common/occ.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import occ

def test_volume_csv_month_clean_sep():
    """
    Test the volume_csv_month_clean_sep function
    """
    csv_data = (
        ',Daily Volume by Exchange,,,\r\n'
        ',Report Date: 10/10/2025,,,\r\n'
        ',,,,\r\n'
        ',,"Date","Equity","Index/Others","Debt","Futures","OCC Total"\r\n'
        ',,"10/10/2025","101,225,615","8,909,809","0","582,737","110,718,161"\r\n'
        ',,"Oct Total","101,225,615","8,909,809","0","582,737","110,718,161"\r\n'
        ',,"YTD Total","10,123,456,789","1,234,567,890","0","123,456,789","11,481,481,468"\r\n'
        ',,"Avg Daily Volume","123,456,789","12,345,678","0","1,234,567","137,037,034"\r\n'
        '\r\n' # Double newline separator
        ',Futures and Options on Futures,,,\r\n'
        ',,"Date","Equity","Index/Others","Debt","Futures","OCC Total"\r\n'
        ',,"10/10/2025","10,122,561","890,980","0","58,273","11,071,816"\r\n'
        ',,"Oct Total","10,122,561","890,980","0","58,273","11,071,816"\r\n'
        ',,"YTD Total","1,012,345,678","123,456,789","0","12,345,678","1,148,148,145"\r\n'
        ',,"Avg Daily Volume","12,345,678","1,234,567","0","123,456","13,703,703"\r\n'
    )
    cleaned_data = occ.volume_csv_month_clean_sep(csv_data)
    assert "contracts" in cleaned_data
    assert "futures" in cleaned_data
    assert ',,"Date","Equity","Index/Others","Debt","Futures","OCC Total"' in cleaned_data["contracts"]
    assert len(cleaned_data["contracts"].splitlines()) == 2  # Header + one data row
    assert "YTD" not in cleaned_data["contracts"]
    assert ',,"Date","Equity","Index/Others","Debt","Futures","OCC Total"' in cleaned_data["futures"]
    assert len(cleaned_data["futures"].splitlines()) == 2  # Header + one data row
    assert "YTD" not in cleaned_data["futures"]
