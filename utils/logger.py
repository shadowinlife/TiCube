import logging
import os
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# log structure
formatter = logging.Formatter(
    "%[(asctime)s levelname:%(levelname)s %(filename)s %(funcName)s %(lineno)d]: %(message)s]")

# console log
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# file log
fileHandler = TimedRotatingFileHandler(os.path.dirname(os.path.abspath(__file__)) + '/server.log', when="D",
                                       interval=5, backupCount=100, encoding="utf-8")
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)