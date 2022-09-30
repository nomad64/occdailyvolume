import logging

import yaml


def yaml_import_config(yaml_file):
    """
    Import yaml config file

    :param yaml_file: yaml file name, or full path if outside of pwd
    :type yaml_file: str
    :return: yaml config
    :rtype: dict
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Loading YAML file: {yaml_file}")
    try:
        yaml_conf = yaml.full_load(open(yaml_file))
    except FileNotFoundError:
        logger.exception(f"Unable to find config file: {yaml_file}")
    return yaml_conf
    

if __name__ == '__main__':
    print("This file cannot be run directly.")
