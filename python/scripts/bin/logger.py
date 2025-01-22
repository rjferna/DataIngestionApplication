import os
import sys
import logging
from datetime import datetime


def set_logger(log_level, log_domain, print_log=True, log_path=None):
    formatter = logging.Formatter(
        "%(asctime)s - %(filename)s[line:%(lineno)d], %(funcName) s[%(levelname)s]: %(message)s"
    )
    level_dict = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }

    logger = logging.getLogger("MainLogger")
    logger.setLevel(level_dict.get(log_level, logging.INFO))

    if not log_path:
        log_path = os.path.dirname(sys.argv[0])
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    file_name = os.path.join(
        log_path, "{:%Y%m%d}_{}.log".format(datetime.now(), log_domain)
    )
    # print("The log file: {}".format(file_name))
    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if print_log:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logger.info("Set logger level to {}".format(log_level))
    return logger


if __name__ == "__main__":
    pass
