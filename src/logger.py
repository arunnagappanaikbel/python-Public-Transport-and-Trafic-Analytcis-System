import logging
import os

def setup_logger(logfile, level=logging.INFO):
    os.makedirs(os.path.dirname(logfile), exist_ok=True)
    logging.basicConfig(
        filename=logfile, 
        level=level, 
        format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    return logging.getLogger(__name__)
    