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


def test_volume_csv_month_clean_sep_new_format():
    """
    Test the volume_csv_month_clean_sep function with the new CSV format (Dec 2025 style)
    """
    csv_data = (
        'Daily OCC Contract Volume - December 2025\r\n'
        'Date,Equity,Index/Others,Debt,Futures,OCC Total\r\n'
        '12/31/2025,"45,727,373","5,185,170","0","123,957","51,036,500",\r\n'
        '12/30/2025,"39,481,630","4,065,421","0","129,459","43,676,510",\r\n'
        'Dec Total,"1,181,470,562","113,033,687","0","3,923,237","1,298,427,486",\r\n'
        'Dec Avg,"53,703,207","5,137,895","0","178,329","59,019,431",\r\n'
        'YTD Total.,"13,949,423,944","1,257,706,797","0","56,740,599","15,263,871,340",\r\n'
        'YTD Avg.,"55,797,696","5,030,827","0","226,962","61,055,485",\r\n'
        '\r\n'
        '\r\n'
        'Daily Futures Contract Volume -December 2025\r\n'
        'Date,Equity,Index/Others,OOF,OCC Total\r\n'
        '12/31/2025,"0","123,957","0","123,957",\r\n'
        '12/30/2025,"0","129,455","4","129,459",\r\n'
        'Dec Total,"0","3,922,362","875","3,923,237",\r\n'
        'Dec Avg,"0","178,289","40","178,329",\r\n'
        'YTD Total.,"0","56,734,835","5,764","56,740,599",\r\n'
        'YTD Avg.,"0","226,939","23","226,962",\r\n'
    )
    cleaned_data = occ.volume_csv_month_clean_sep(csv_data)
    assert "contracts" in cleaned_data
    assert "futures" in cleaned_data
    
    # Check contracts section
    assert 'Date,Equity,Index/Others,Debt,Futures,OCC Total' in cleaned_data["contracts"]
    # 2 data rows + 1 header = 3 lines.
    # But wait, filtering happens.
    # The summary lines (Dec Total, Dec Avg, YTD...) should be removed.
    # "Dec" is removed because month is December.
    # "YTD" is removed.
    # "Avg" is removed.
    # So only 2 data lines remain + header?
    # Actually, verify what stays.
    lines = cleaned_data["contracts"].splitlines()
    assert len(lines) == 3 # Header + 2 data rows
    assert "12/31/2025" in lines[1]
    assert "12/30/2025" in lines[2]
    assert "Dec Total" not in cleaned_data["contracts"]
    assert "YTD Total" not in cleaned_data["contracts"]

    # Check futures section
    assert 'Date,Equity,Index/Others,OOF,OCC Total' in cleaned_data["futures"]
    lines_futures = cleaned_data["futures"].splitlines()
    assert len(lines_futures) == 3 # Header + 2 data rows
    assert "12/31/2025" in lines_futures[1]
    assert "Dec Total" not in cleaned_data["futures"]
