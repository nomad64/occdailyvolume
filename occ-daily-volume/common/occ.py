"""
Functions for interacting with theocc.com
"""
import io
import logging
from datetime import date, datetime
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

# HTTP request timeout in seconds
REQUEST_TIMEOUT = 30


def volume_csv_month_get(req_url: str, req_date: date, req_format: str) -> str:
    """
    Get volume data from theocc.com for the given month.

    :param req_url: url for the request
    :type req_url: str
    :param req_date: date to request, must include year, month, and day
    :type req_date: date
    :param req_format: return format of data (only CSV is supported)
    :type req_format: str
    :return: volume data
    :rtype: str
    """
    if not isinstance(req_date, date):
        raise TypeError("req_date must be type: date")
    req_date += relativedelta(day=1)
    req_params = {
        "reportDate": req_date.strftime("%Y%m%d"),
        "format": req_format.lower(),
    }
    baseurl = urljoin(req_url, "/")
    logger.debug(
        f"Retrieving monthly volume report for {req_date.strftime('%B %Y')}" f" from {baseurl}"
    )
    try:
        r = requests.get(f"{req_url}?{urlencode(req_params)}", timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except requests.exceptions.Timeout:
        raise TimeoutError(f"Request timed out after {REQUEST_TIMEOUT} seconds")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to fetch data from {baseurl}: {e}")

    if "Invalid report Date" in r.text:
        raise ValueError("given req_date returned invalid response")
    if "Report is not available" in r.text:
        raise ValueError("given req_date is not publically available")
    return r.text


def volume_csv_month_clean_sep(csv_data: str) -> dict:
    """
    Clean CSV data to be loaded into pandas, splitting out the headers and tables (since we actually get two CSVs from OCC)

    :param csv_data: CSV output from OCC
    :type csv_data: str
    :return: cleaned and sorted volume information
    :rtype: dict
    """
    csv_clean = []
    volume_dict = {}
    bad_lines = ["YTD", "Avg"]
    csv_data = csv_data.replace(",\r\n", "\r\n")
    csv_list = csv_data.split("\r\n")
    report_date = datetime.strptime(csv_list[1].split(": ")[1].split(",")[0], "%m/%d/%Y").date()
    # Append the month short name to bad_lines
    bad_lines.append(report_date.strftime("%b"))
    # Filter out lines that are not part of the actual data tables or are summary rows
    # Keep the main headers and data rows, remove empty lines and summary lines
    filtered_lines = []
    in_contracts_section = False
    in_futures_section = False

    for line in csv_list:
        if "Daily Volume by Exchange" in line:
            in_contracts_section = True
            in_futures_section = False
            continue
        if "Futures and Options on Futures" in line:
            in_contracts_section = False
            in_futures_section = True
            filtered_lines.append('') # Add a blank line to act as a separator for csv_split
            continue

        if in_contracts_section or in_futures_section:
            if line.strip() == '' or line.strip() == ',,,': # Remove empty lines or lines with just commas
                continue
            if any(b in line for b in bad_lines): # Remove summary lines
                continue
            if "Report Date" in line: # Remove report date line
                continue
            filtered_lines.append(line)

    csv_clean = filtered_lines
    csv_split = "\n".join(csv_clean).split("\n\n")
    volume_dict = {
        "contracts": csv_split[0],
        "contracts_headers": csv_split[0].split("\n")[0].split(","),
        "futures": csv_split[1],
        "futures_headers": csv_split[1].split("\n")[1].split(","),
    }
    logger.debug(f"Successfully cleaned data for {report_date.strftime('%B %Y')}")
    return volume_dict


def volume_df_create(vol_dict: dict, merge_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Create dataframe from cleaned CSV dict. Optionally merge the data into one dataframe.

    :param vol_dict: output from volume_csv_month_clean_sep
    :type vol_dict: dict
    """
    vol_df = pd.read_csv(
        io.StringIO(vol_dict["contracts"]),
        thousands=",",
        index_col="Date",
        parse_dates=["Date"],
    )
    if merge_df:
        return pd.concat([vol_df, merge_df])
    return vol_df


def get_volume_by_month_to_df(req_url: str, req_date: date, req_format: str):
    """
    Helper function to get monthly volume into dataframe.

    :param req_url: url for the request
    :type req_url: str
    :param req_date: date to request, must include year, month, and day
    :type req_date: date
    :param req_format: return format of data (only CSV is supported)
    :type req_format: str
    :return: volume data
    :rtype: str
    """
    csv_raw = volume_csv_month_get(
        req_url=req_url, req_date=req_date, req_format=req_format
    )
    volume_dict = volume_csv_month_clean_sep(csv_raw)
    volume_df = volume_df_create(volume_dict)
    return volume_df
