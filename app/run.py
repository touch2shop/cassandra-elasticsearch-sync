import logging
import sys
from app.settings import Settings
from app.sync_loop import SyncLoop


_LOG_FILE_NAME = "cassandra-elasticsearch-sync.log"
_SETTINGS_FILE_NAME = "settings.yaml"


def setup_logger():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("'%(asctime)s - %(levelname)s - %(message)s")

    log_file_handler = logging.FileHandler(_LOG_FILE_NAME)
    log_file_handler.setLevel(logging.INFO)
    log_file_handler.setFormatter(formatter)
    root_logger.addHandler(log_file_handler)

    log_stream_handler = logging.StreamHandler(sys.stdout)
    log_stream_handler.setLevel(logging.INFO)
    log_stream_handler.setFormatter(formatter)
    root_logger.addHandler(log_stream_handler)

def run():
    setup_logger()
    settings = Settings.load_from_file(_SETTINGS_FILE_NAME)
    sync_loop = SyncLoop(settings)
    sync_loop.run()

if __name__ == "__main__":
    run()
