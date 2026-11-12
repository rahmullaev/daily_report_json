import logging
from settings import LOG_FILE

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s]-%(levelname)s-%(message)s',
        datefmt='%d.%m.%Y %H:%M:%S',
        handlers=[
            logging.FileHandler(str(LOG_FILE), encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


log = setup_logging()
