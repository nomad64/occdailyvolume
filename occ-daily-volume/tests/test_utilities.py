"""
Tests for common utility modules: dataframe.py, logging.py, and yaml.py
"""
import sys
import os
import logging
import tempfile
import yaml
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import dataframe
from common import logging as common_logging
from common import yaml as common_yaml

# --- common.dataframe tests ---

def test_pretty_print_df(capsys):
    """Test pretty_print_df outputs formatted table"""
    df = pd.DataFrame({
        'A': [1000, 2000],
        'B': [1.5, 2.5]
    }, index=pd.to_datetime(['2024-01-01', '2024-01-02']))
    
    dataframe.pretty_print_df(df)
    
    captured = capsys.readouterr()
    output = captured.out
    
    # Check for table borders
    assert "+-" in output
    assert "-+" in output
    
    # Check for formatted date
    assert "2024-01-01" in output
    
    # Check for formatted numbers (thousands separator)
    assert "1,000" in output
    assert "2,000" in output
    
    # Check for position index (column with empty name)
    assert " 1 |" in output or "| 1 |" in output

# --- common.logging tests ---

@patch('logging.config.dictConfig')
def test_setup_logging(mock_dict_config):
    """Test setup_logging configures logging correctly"""
    log_name = "test_app"
    log_level = "DEBUG"
    
    filename = common_logging.setup_logging(log_name, log_level)
    
    # Check that a filename was returned
    assert filename.startswith(f"{log_name}_")
    assert filename.endswith(".log")
    
    # Verify dictConfig was called
    mock_dict_config.assert_called_once()
    config_args = mock_dict_config.call_args[0][0]
    
    # Check config details
    assert config_args['handlers']['console']['level'] == logging.DEBUG
    assert config_args['handlers']['file_handler']['filename'] == f"/tmp/{filename}"

@patch('logging.config.dictConfig')
def test_setup_logging_default_level(mock_dict_config):
    """Test setup_logging defaults to INFO"""
    common_logging.setup_logging("test_app")
    config_args = mock_dict_config.call_args[0][0]
    assert config_args['handlers']['console']['level'] == logging.INFO

# --- common.yaml tests ---

def test_yaml_import_config_success():
    """Test successful YAML loading"""
    data = {"key": "value", "nested": {"a": 1}}
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
        yaml.dump(data, tmp)
        tmp_path = tmp.name
        
    try:
        config = common_yaml.yaml_import_config(tmp_path)
        assert config == data
    finally:
        os.remove(tmp_path)

def test_yaml_import_config_file_not_found():
    """Test YAML loading with non-existent file"""
    with pytest.raises(FileNotFoundError):
        common_yaml.yaml_import_config("/nonexistent/file.yaml")
