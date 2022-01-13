import logging


class ExtraDataFilter(logging.Filter):
    def filter(self, record):
        if not getattr(record, "global_id", None):
            record.global_id = "no_global_id"
        return True


FORMAT = (
    "[%(levelname)s]\t[%(asctime)s]\t[%(module)s:%(funcName)s:%(lineno)d]\t-\t"
    "%(message)s\t(GID: %(global_id)s)"
)

logging.basicConfig(format=FORMAT)

__logger = logging.getLogger(__name__)
__logger.setLevel(logging.DEBUG)

for name in logging.root.manager.loggerDict:
    logging.getLogger(name).addFilter(ExtraDataFilter())


def setup_logger():
    return __logger
