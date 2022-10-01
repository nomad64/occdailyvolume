import logging
import sqlite3 as sql

import pandas as pd


def db_write_df_to_sql(db_filepath: str, db_table: str, df_: pd.DataFrame):
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
    df_len = len(df_)
    logger.debug(f"Starting write of {df_len:,} rows to {db_filepath}")
    df_.to_sql(name=db_table, con=conn)
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
    conn = sql.connect(db_filepath)
    logger.debug(f"Attemping to read DB {db_table} from file {db_filepath}")
    out_df = pd.read_sql_query(f"SELECT * from {db_table}", conn, index_col="Date", parse_dates=["Date"])
    logger.debug(f"Successfully read {len(out_df)} rows")
    return out_df


if __name__ == "__main__":
    print("This file cannot be run directly.")
