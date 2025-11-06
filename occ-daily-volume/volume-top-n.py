"""
Main script for the OCC Daily Volume data pipeline.

This script serves as the command-line interface to fetch, update, and display
the top N daily volume statistics from the Options Clearing Corporation (OCC).

Project available on GitHub: https://github.com/nomad64/occdailyvolume
"""

import argparse
import logging
import os

import common.dataframe
import common.logging
import common.sqlite
import common.updater
import common.yaml


def main(args_):
    yaml_conf = common.yaml.yaml_import_config(args_.config)
    if args_.database:
        database_filepath = args_.database
    else:
        database_filepath = yaml_conf["database"]["sqlite"]["db_filepath"]
    if args_.update:
        common.updater.backfill_db_to_previous_month(
            req_url=yaml_conf["occweb"]["daily_volume_url"],
            req_format=yaml_conf["occweb"]["daily_volume_format"],
            db_filepath=database_filepath,
            db_table=yaml_conf["database"]["sqlite"]["db_table"],
        )
    volume_df = common.sqlite.db_read_sql_to_df(
        db_filepath=database_filepath,
        db_table=yaml_conf["database"]["sqlite"]["db_table"],
    )
    common.dataframe.pretty_print_df(volume_df.nlargest(args_.number, "OCC Total"))


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
    parser.add_argument(
        "-l",
        "--log-level",
        metavar="LEVEL",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    args = parser.parse_args()
    common.logging.setup_logging(os.path.splitext(os.path.basename(__file__))[0], args.log_level)
    logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
    main(args)
