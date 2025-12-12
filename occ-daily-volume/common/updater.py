"""
Functions for updating the local database
"""
import logging
from datetime import date

from dateutil.relativedelta import relativedelta

import common.occ
import common.sqlite

logger = logging.getLogger(__name__)


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
            month_df = common.occ.get_volume_by_month_to_df(
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
            (backfill_min_end_date_delta.years * 12)
            + backfill_min_end_date_delta.months
        )
        logger.info(f"Backfilling {backfill_months_difference} months")
        working_month = db_df_min_date - relativedelta(months=1)
        working_month += relativedelta(day=1)
        while working_month > backfill_end_date:
            try:
                month_df = common.occ.get_volume_by_month_to_df(
                    req_url=req_url, req_date=working_month, req_format=req_format
                )
                common.sqlite.db_write_df_to_sql(
                    db_filepath=db_filepath, db_table=db_table, df_to_write=month_df
                )
            except ValueError as e:
                logger.warning(
                    f"Data unavailable for {working_month.strftime('%B %Y')}, skipping: {e}"
                )
            except (TimeoutError, ConnectionError) as e:
                logger.error(
                    f"Network error for {working_month.strftime('%B %Y')}, skipping: {e}"
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
                try:
                    month_df = common.occ.get_volume_by_month_to_df(
                        req_url=req_url, req_date=working_month, req_format=req_format
                    )
                    common.sqlite.db_write_df_to_sql(
                        db_filepath=db_filepath, db_table=db_table, df_to_write=month_df
                    )
                except ValueError as e:
                    logger.warning(
                        f"Data unavailable for {working_month.strftime('%B %Y')}, skipping: {e}"
                    )
                except (TimeoutError, ConnectionError) as e:
                    logger.error(
                        f"Network error for {working_month.strftime('%B %Y')}, skipping: {e}"
                    )
                working_month += relativedelta(months=1)
            logger.debug(f"DB filled up to {prev_month.strftime('%B %Y')}")
        else:
            logger.debug("DB is already current")
    else:
        logger.debug("DB was empty and successfully backfilled, not filling")
