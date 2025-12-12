import logging
import re
import sqlite3 as sql
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _validate_table_name(table_name: str) -> None:
    """
    Validate SQL table name to prevent SQL injection.

    :param table_name: table name to validate
    :type table_name: str
    :raises ValueError: if table name contains invalid characters
    """
    if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
        raise ValueError(
            f"Invalid table name '{table_name}'. Only alphanumeric characters and underscores are allowed."
        )


def db_write_df_to_sql(db_filepath: str, db_table: str, df_to_write: pd.DataFrame) -> None:
    """
    Write given dataframe to SQLite DB file

    :param db_filepath: database filepath
    :type db_filepath: str
    :param db_table: database table to write
    :type db_table: str
    :param df_to_write: dataframe to write
    :type df_to_write: pd.DataFrame
    :return: None
    """
    _validate_table_name(db_table)
    df_len = len(df_to_write)
    logger.debug(f"Starting write of {df_len:,} rows to {db_filepath}")

    with sql.connect(db_filepath) as conn:
        df_to_write.to_sql(name=db_table, con=conn, if_exists="append")

    logger.debug(f"Successfully wrote to DB {db_filepath}")


def db_read_sql_to_df(db_filepath: str, db_table: str) -> pd.DataFrame:
    """
    Read given DB file into dataframe

    :param db_filepath: database filepath
    :type db_filepath: str
    :param db_table: database table to read
    :type db_table: str
    :return: dataframe containing database contents
    :rtype: pd.DataFrame
    """
    _validate_table_name(db_table)

    if Path(db_filepath).is_file():
        logger.debug(f"Attemping to read DB {db_table} from file {db_filepath}")

        with sql.connect(db_filepath) as conn:
            out_df = pd.read_sql_query(
                f"SELECT * from {db_table}",
                conn,
                index_col="Date",
                parse_dates=["Date"],
            )

        logger.debug(f"Successfully read {len(out_df)} rows")
    else:
        logger.warning(f"Unable to find {db_filepath}, returning empty dataframe")
        out_df = pd.DataFrame()

    return out_df


if __name__ == "__main__":
    print("This file cannot be run directly.")
