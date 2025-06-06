
import logging

logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def log_info(msg):
    logging.info(msg)

def log_error(msg):
    logging.error(msg)

def log_warning(msg):
    logging.warning(msg)
