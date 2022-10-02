import logging
import sqlite3 as sql
from pathlib import Path

import pandas as pd
from pandas.io.sql import DatabaseError


def db_write_df_to_sql(db_filepath: str, db_table: str, df_to_write: pd.DataFrame):
    """
    Write given dataframe to SQLite DB file

    :param db_filepath: database filepath
    :type db_filepath: str
    :param db_table: database table to write
    :type db_table: str
    :param df_: dataframe to write
    :type df_: pd.DataFrame
    """
    logger = logging.getLogger(__name__)
    conn = sql.connect(db_filepath)
    df_len = len(df_to_write)
    logger.debug(f"Starting write of {df_len:,} rows to {db_filepath}")
    df_to_write.to_sql(name=db_table, con=conn, if_exists="append")
    logger.debug(f"Successfully wrote to DB {db_filepath}")
    conn.close()


def db_read_sql_to_df(db_filepath: str, db_table: str) -> pd.DataFrame:
    """
    Read given DB file into dataframe

    :param db_filepath: database filepath
    :type db_filepath: str
    :param db_table: database table to read
    :type db_table: str
    """
    logger = logging.getLogger(__name__)
    if Path(db_filepath).is_file():
        conn = sql.connect(db_filepath)
        logger.debug(f"Attemping to read DB {db_table} from file {db_filepath}")
        out_df = pd.read_sql_query(
            f"SELECT * from {db_table}",
            conn,
            index_col="Date",
            parse_dates={"Date": "%Y-%m-%d"},
        )
        logger.debug(f"Successfully read {len(out_df)} rows")
    else:
        logger.warning(f"Unable to find {db_filepath}, returning empty dataframe")
        out_df = pd.DataFrame()
    return out_df


if __name__ == "__main__":
    print("This file cannot be run directly.")
