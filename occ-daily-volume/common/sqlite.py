import sqlite3 as sql

def db_write_df_to_sql(db_filepath: str, db_table: str, df_: pd.DataFrame):
    conn = sql.connect(db_filepath)
    df_len = len(df_)
    logger.debug(f"Starting write of {df_len:,} rows to {db_filepath}")
    df_.to_sql(name=db_table, con=conn)
    logger.debug(f"Successfully wrote to DB {db_filepath}")
    conn.close()


def db_read_sql_to_df(db_filepath: str, db_table: str):
    conn = sql.connect(db_filepath)
    logger.debug(f"")
    pd.read_sql_query("SELECT * from volHist", conn)

if __name__ == '__main__':
    print("This file cannot be run directly.")
