import logging

from config import settings

FORMAT = "[%(levelname)s]\t[%(asctime)s]\t[%(module)s:%(funcName)s:%(lineno)d]\t-\t%(message)s"

logging.basicConfig(format=FORMAT)

__logger = logging.getLogger(__name__)
__logger.setLevel(logging.INFO)
if settings.debug:
    __logger.setLevel(logging.DEBUG)


def setup_logger():
    return __logger
