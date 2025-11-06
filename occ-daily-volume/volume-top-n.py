"""
Build Top 10 list from daily volume
"""

import argparse
import io
import logging
import os
from datetime import date, datetime
from urllib.parse import urlencode, urljoin

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

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
        raise (TypeError("req_date must be type: date"))
    req_date += relativedelta(day=1)
    req_params = {
        "reportDate": req_date.strftime("%Y%m%d"),
        "format": req_format.lower(),
    }
    baseurl = urljoin(req_url, "/")
    logger.debug(
        f"Retrieving monthly volume report for {req_date.strftime('%B %Y')} from {baseurl}"
    )
    r = requests.get(f"{req_url}?{urlencode(req_params)}")
    r.raise_for_status()
    if "Invalid report Date" in r.text:
        raise (ValueError("given req_date returned invalid response"))
    if "Report is not available" in r.text:
        raise (ValueError("given req_date is not publically available"))
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
    csv_list = csv_data.split("\r\n")
    report_date = datetime.strptime(csv_list[5].split(",")[0], "%m/%d/%Y").date()
    # Append the month short name to bad_lines
    bad_lines.append(report_date.strftime("%b"))
    csv_clean = [i for i in csv_list if not any(b in i for b in bad_lines)]
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


def backfill_db_to_previous_month(
    req_url: str, req_format: str, db_filepath: str, db_table: str
):
    """
    Fill and backfill database file to include all publically available data.

    :param req_url: url for the request
    :type req_url: str
    :param req_format: return format of data (only CSV is supported)
    :type req_format: str
    :param db_filepath: database filepath
    :type db_filepath: str
    :param db_table: database table to read
    :type db_table: str
    """
    backfill_end_date = date(2008, 1, 1)
    prev_month = date.today() + relativedelta(day=1) - relativedelta(months=1)
    logger.debug("Reading DB to find known range")
    db_df = common.sqlite.db_read_sql_to_df(db_filepath=db_filepath, db_table=db_table)
    if len(db_df) == 0:
        db_df_max_date = None
        db_df_min_date = prev_month
    else:
        db_df_max_date = db_df.index.max().date()
        db_df_min_date = db_df.index.min().date()
    # Fill database moving backward in time
    if db_df_min_date > backfill_end_date:
        if db_df_min_date == prev_month:
            logger.debug(
                f"DB {db_filepath} appears to be empty, fetching {prev_month.strftime('%B %Y')}"
            )
            month_df = get_volume_by_month_to_df(
                req_url=req_url, req_date=prev_month, req_format=req_format
            )
            common.sqlite.db_write_df_to_sql(
                db_filepath=db_filepath, db_table=db_table, df_to_write=month_df
            )
        logger.debug(
            f"DB has data back to {db_df_min_date.strftime('%B %Y')}, backfill end date is {backfill_end_date.strftime('%B %Y')}"
        )
        backfill_min_end_date_delta = relativedelta(db_df_min_date, backfill_end_date)
        backfill_months_difference = (
            backfill_min_end_date_delta.years * 12
        ) + backfill_min_end_date_delta.months
        logger.info(f"Backfilling {backfill_months_difference} months")
        working_month = db_df_min_date - relativedelta(months=1)
        working_month += relativedelta(day=1)
        while working_month > backfill_end_date:
            try:
                month_df = get_volume_by_month_to_df(
                    req_url=req_url, req_date=working_month, req_format=req_format
                )
                common.sqlite.db_write_df_to_sql(
                    db_filepath=db_filepath, db_table=db_table, df_to_write=month_df
                )
            except ValueError:
                logger.warning(
                    f"Data unavailable for {working_month.strftime('%B %Y')}, skipping"
                )
            working_month -= relativedelta(months=1)
        logger.debug(
            f"DB backfilled successfully to {backfill_end_date.strftime('%B %Y')}"
        )
    else:
        logger.debug("DB is already backfilled")
    # Fill database moving forward in time
    if db_df_max_date:
        logger.debug(
            f"DB has data upto {db_df_max_date.strftime('%B %Y')}, previous month is {prev_month.strftime('%B %Y')}"
        )
        if db_df_max_date <= prev_month:
            fill_max_prev_date_delta = relativedelta(prev_month, db_df_max_date)
            fill_months_difference = (
                (fill_max_prev_date_delta.years * 12)
                + fill_max_prev_date_delta.months
                + 1
            )
            logger.info(f"Filling {fill_months_difference} months")
            working_month = db_df_max_date + relativedelta(months=1)
            working_month += relativedelta(day=1)
            while working_month <= prev_month:
                month_df = get_volume_by_month_to_df(
                    req_url=req_url, req_date=working_month, req_format=req_format
                )
                common.sqlite.db_write_df_to_sql(
                    db_filepath=db_filepath, db_table=db_table, df_to_write=month_df
                )
                working_month += relativedelta(months=1)
            logger.debug(f"DB filled up to {prev_month.strftime('%B %Y')}")
        else:
            logger.debug("DB is already current")
    else:
        logger.debug("DB was empty and successfully backfilled, not filling")


def main(args_):
    yaml_conf = common.yaml.yaml_import_config(args_.config)
    if args_.database:
        database_filepath = args_.database
    else:
        database_filepath = yaml_conf["database"]["sqlite"]["db_filepath"]
    if args_.update:
        backfill_db_to_previous_month(
            req_url=yaml_conf["occweb"]["daily_volume_url"],
            req_format=yaml_conf["occweb"]["daily_volume_format"],
            db_filepath=database_filepath,
            db_table=yaml_conf["database"]["sqlite"]["db_table"],
        )
    volume_df = common.sqlite.db_read_sql_to_df(
        db_filepath=database_filepath,
        db_table=yaml_conf["database"]["sqlite"]["db_table"],
    )
    print(volume_df.nlargest(args_.number, "OCC Total"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a high volume days list")
    parser.add_argument(
        "-C",
        "--config",
        metavar="filename",
        type=str,
        default=f"{os.path.splitext(os.path.basename(__file__))[0]}.yaml",
        help="Specify an alternate config file to use",
    )
    parser.add_argument(
        "-D",
        "--database",
        metavar="filepath",
        type=str,
        help="Specify an alternate database file to use",
    )
    parser.add_argument(
        "-n",
        "--number",
        metavar="N",
        type=int,
        default=10,
        help="Number of top volume days to return",
    )
    parser.add_argument(
        "-u",
        "--update",
        action="store_true",
        help="Update local database before analysis",
    )
    args = parser.parse_args()
    common.logging.setup_logging(os.path.splitext(os.path.basename(__file__))[0])
    logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
    main(args)
