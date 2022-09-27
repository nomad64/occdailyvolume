"""
Build Top 10 list from daily volume
https://marketdata.theocc.com/daily-volume-statistics?reportDate=20190501&format=csv
"""

import argparse
import os
import sqlite3 as sql
import common.logging

import pandas as pd


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
    