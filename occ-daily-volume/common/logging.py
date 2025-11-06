import logging
import logging.config
from datetime import datetime


def setup_logging(name_, log_level: str = "INFO"):
    """
    Setup simple console and file logging

    :param name_: name of app using logging, ideally should be __name__
    :param log_level: logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :type log_level: str
    :return:
    """
    numeric_log_level = getattr(logging, log_level.upper(), logging.INFO)
    log_file_name = f"{name_}_{datetime.now().strftime('%m-%d-%Y_%H%M%S')}.log"
    CONFIG = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "syslog-standard": {
                "class": "logging.Formatter",
                "datefmt": "%b %d %H:%M:%S",
                "format": "%(asctime)s %(name)s[%(process)d]: %(message)s",
            }
        },
        "handlers": {
            "console": {
                "level": numeric_log_level,
                "class": "logging.StreamHandler",
                "formatter": "syslog-standard",
                "stream": "ext://sys.stdout",
            },
            "file_handler": {
                "level": numeric_log_level,
                "class": "logging.handlers.WatchedFileHandler",
                "formatter": "syslog-standard",
                "filename": f"/tmp/{log_file_name}",
                "mode": "a",
                "encoding": "utf-8",
            },
        },
        "loggers": {},
        "root": {"handlers": ["console", "file_handler"], "level": numeric_log_level},
    }
    logging.config.dictConfig(CONFIG)
    logger = logging.getLogger(__name__)
    logger.info("logging initialized successfully.")
    return log_file_name


if __name__ == "__main__":
    print("This file cannot be run directly.")
