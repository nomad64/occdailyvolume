"""
Tests for network error handling in common/occ.py
"""
import sys
import os
from datetime import date
import pytest
from unittest.mock import patch, Mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import occ
import requests


def test_volume_csv_month_get_timeout():
    """Test that timeout exception is properly handled and re-raised"""
    test_date = date(2024, 1, 1)

    with patch('common.occ.requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.Timeout()

        with pytest.raises(TimeoutError, match="Request timed out after 30 seconds"):
            occ.volume_csv_month_get("http://test.com", test_date, "csv")


def test_volume_csv_month_get_connection_error():
    """Test that connection errors are properly handled and re-raised"""
    test_date = date(2024, 1, 1)

    with patch('common.occ.requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.ConnectionError("Network unreachable")

        with pytest.raises(ConnectionError, match="Failed to fetch data from"):
            occ.volume_csv_month_get("http://test.com", test_date, "csv")


def test_volume_csv_month_get_http_error():
    """Test that HTTP errors are properly handled"""
    test_date = date(2024, 1, 1)

    with patch('common.occ.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(ConnectionError, match="Failed to fetch data from"):
            occ.volume_csv_month_get("http://test.com", test_date, "csv")


def test_volume_csv_month_get_timeout_value():
    """Test that request timeout is set to 30 seconds"""
    test_date = date(2024, 1, 1)

    with patch('common.occ.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "Valid CSV data"
        mock_get.return_value = mock_response

        occ.volume_csv_month_get("http://test.com", test_date, "csv")

        # Verify timeout was passed
        assert mock_get.called
        call_kwargs = mock_get.call_args.kwargs
        assert 'timeout' in call_kwargs
        assert call_kwargs['timeout'] == 30


def test_volume_csv_month_get_invalid_date_type():
    """Test that TypeError is raised for invalid date type"""
    with pytest.raises(TypeError, match="req_date must be type: date"):
        occ.volume_csv_month_get("http://test.com", "2024-01-01", "csv")

    with pytest.raises(TypeError, match="req_date must be type: date"):
        occ.volume_csv_month_get("http://test.com", None, "csv")


def test_volume_csv_month_get_invalid_report_date():
    """Test that ValueError is raised for invalid report date"""
    test_date = date(2024, 1, 1)

    with patch('common.occ.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "Invalid report Date"
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="given req_date returned invalid response"):
            occ.volume_csv_month_get("http://test.com", test_date, "csv")


def test_volume_csv_month_get_report_not_available():
    """Test that ValueError is raised when report is not available"""
    test_date = date(2024, 1, 1)

    with patch('common.occ.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = "Report is not available"
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="given req_date is not publically available"):
            occ.volume_csv_month_get("http://test.com", test_date, "csv")


def test_volume_csv_month_get_success():
    """Test successful request with proper timeout"""
    test_date = date(2024, 1, 1)
    expected_text = "Valid,CSV,Data\n1,2,3"

    with patch('common.occ.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.text = expected_text
        mock_get.return_value = mock_response

        result = occ.volume_csv_month_get("http://test.com", test_date, "csv")

        assert result == expected_text
        assert mock_get.called
        # Verify timeout was passed
        call_kwargs = mock_get.call_args.kwargs
        assert call_kwargs['timeout'] == occ.REQUEST_TIMEOUT
