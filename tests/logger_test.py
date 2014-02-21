__author__ = 'michael'

import os
import time
import logging
import logging.config
import yaml
import zkclient


def Main():
    with open('logger.yml') as f:
        config = yaml.load(f)
    logging.config.dictConfig(config)
    logger = logging.getLogger('py_zkclient')
    logger.debug('hello world')


if __name__ == '__main__':
    Main()