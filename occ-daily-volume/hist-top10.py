"""
Build Top 10 list from daily volume
"""

import argparse
import os
import sqlite3 as sql
from datetime import date
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests

import common.logging


def get_volume_csv_month(req_url: str, req_date: date, req_format: str):
    if not isinstance(req_date, date):
        raise(TypeError(f"req_date must be type: date"))
    req_params = {
        "reportDate": req_date.strftime('%Y%m%d'),
        "format": req_format,
        }
    r = requests.get(f"{req_url}?{urlencode(req_params)}")
    # print(f"{req_url}?{urlencode(req_params)}")
    r.raise_for_status()
    if "Invalid report Date" in r.text:
        raise(ValueError("given req_date returned invalid response"))
    return r.text


def volume_csv_month_clean_sep(csv_data):
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


def main(args_):
    print(args_)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate a high volume days list')
    parser.add_argument("-C", "--config", metavar="filename", type=str, default=f"{os.path.splitext(os.path.basename(__file__))[0]}.yaml", help="Specify a config file to use.")
    # mode = parser.add_subparsers(title='actions', help='Select at least one action for helper to perform', required=True)
    # test1 = mode.add_parser('test1', help='test1 help')
    # test2 = mode.add_parser('test2', help='test1 help')
    args = parser.parse_args()
    common.logging.setup_logging(os.path.splitext(os.path.basename(__file__))[0])
    main(args)
    