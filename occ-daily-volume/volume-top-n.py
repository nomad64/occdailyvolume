"""
Build Top 10 list from daily volume
"""

import argparse
import io
import logging
import os
from datetime import date, datetime
from urllib.parse import urlencode

import pandas as pd
import requests

import common.logging
import common.sqlite
import common.yaml


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
        raise(TypeError(f"req_date must be type: date"))
    req_params = {
        "reportDate": req_date.strftime('%Y%m%d'),
        "format": req_format.lower(),
        }
    r = requests.get(f"{req_url}?{urlencode(req_params)}")
    r.raise_for_status()
    if "Invalid report Date" in r.text:
        raise(ValueError("given req_date returned invalid response"))
    if "Report is not available" in r.text:
        raise(ValueError("given req_date is not publically available"))
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
    bad_lines = ["YTD", "Avg", "Daily"]
    csv_data = csv_data.replace(",\r\n", "\r\n")
    csv_list = csv_data.split('\r\n')
    # Append the month short name to bad_lines
    bad_lines.append(datetime.strptime(csv_list[5].split(",")[0], "%m/%d/%Y").date().strftime("%b"))
    csv_clean = [i for i in csv_list if not any(b in i for b in bad_lines)]
    csv_split = "\n".join(csv_clean).split("\n\n")
    volume_dict = {
        "contracts": csv_split[0],
        "contracts_headers": csv_split[0].split("\n")[0].split(","),
        "futures": csv_split[1],
        "futures_headers": csv_split[1].split("\n")[1].split(","),
    }
    return volume_dict


def volume_df_create(vol_dict: dict, merge_df: pd.DataFrame=None) -> pd.DataFrame:
    """
    Create dataframe from cleaned CSV dict. Optionally merge the data into one dataframe.

    :param vol_dict: output from volume_csv_month_clean_sep
    :type vol_dict: dict
    """
    vol_df = pd.read_csv(io.StringIO(vol_dict['contracts']), thousands=",", index_col="Date", parse_dates=["Date"])
    if merge_df:
        output_df = pd.concat([vol_df, merge_df])
        return merge_df
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
    csv_raw = volume_csv_month_get(req_url=req_url, req_date=req_date, req_format=req_format)
    volume_dict = volume_csv_month_clean_sep(csv_raw)
    volume_df = volume_df_create(volume_dict)
    return volume_df


def main(args_):
    yaml_conf = common.yaml.yaml_import_config(args_.config)
    volume_df = get_volume_by_month_to_df(req_url=yaml_conf['occweb']['daily_volume_url'],
                                        req_date=date(2021, 12, 1),
                                        req_format=yaml_conf['occweb']['daily_volume_format'])
    print(volume_df.nlargest(args_.number, "OCC Total"))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate a high volume days list')
    parser.add_argument("-C", "--config", metavar="filename", type=str, default=f"{os.path.splitext(os.path.basename(__file__))[0]}.yaml", help="Specify a config file to use.")
    parser.add_argument("-n", "--number", metavar="n", type=int, default=10)
    # mode = parser.add_subparsers(title='actions', help='Select at least one action for helper to perform', required=True)
    # test1 = mode.add_parser('test1', help='test1 help')
    # test2 = mode.add_parser('test2', help='test1 help')
    args = parser.parse_args()
    common.logging.setup_logging(os.path.splitext(os.path.basename(__file__))[0])
    logging = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
    main(args)
    