import time

import logging
from logging.handlers import TimedRotatingFileHandler

# format the log entries
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s - %(message)s')

handler = TimedRotatingFileHandler('/opt/ExcelBackup/logfile.log',
                                   when='midnight',
                                   backupCount=10)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

