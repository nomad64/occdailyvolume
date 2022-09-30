import logging
import logging.config
import os
from datetime import datetime


def setup_logging(name_):
    """
    Setup simple console and file logging

    :param name_: name of app using logging, ideally should be __name__
    :return:
    """
    log_file_name = f"{name_}_{datetime.now().strftime('%m-%d-%Y_%H%M%S')}.log"
    CONFIG = {'version': 1,
              'disable_existing_loggers': True,
              'formatters': {
                  'syslog-standard': {
                      'class': 'logging.Formatter',
                      'datefmt': '%b %d %H:%M:%S',
                      'format': '%(asctime)s %(name)s[%(process)d]: %(message)s'
                      }
                  },
              'handlers': {
                  'console': {
                      'level': 'WARNING',
                      'class': 'logging.StreamHandler',
                      'formatter': 'syslog-standard',
                      'stream': 'ext://sys.stdout'
                      },
                  'file_handler': {
                      'level': 'DEBUG',
                      'class': 'logging.handlers.WatchedFileHandler',
                      'formatter': 'syslog-standard',
                      'filename': f'/tmp/{log_file_name}',
                      'mode': 'a',
                      'encoding': 'utf-8'
                      }
                  },
              'loggers': {},
              'root': {
                  'handlers': ['console', 'file_handler'],
                  'level': 'DEBUG'
                  }
              }
    logging.config.dictConfig(CONFIG)
    logging.info('logging initialized successfully.')
    return log_file_name


if __name__ == '__main__':
    print("This file cannot be run directly.")
