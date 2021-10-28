import logging
import os
from pathlib import Path

from skopeutils.cache_manager import CacheManager
from skopeutils.config_reader import ConfigReader


__all__ = [
    "ROOT_DIR", "CONFIG_DIR", "DATA_REPO", "VENV_DIR",
    "config", "pipeline_cache", "logger"
]

def create_data_folder():
    """
    Creates folder under user's home folder for data storage

    Returns:
         None
    """
    data_path = os.path.join(os.path.expanduser("~"), "covid_data")
    if not os.path.exists(data_path):
        os.makedirs(data_path)
        logger.info(f"Created data folder at location {data_path}")
    return data_path


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(ROOT_DIR, 'config')
DATA_REPO = create_data_folder()
VENV_DIR = os.path.join(ROOT_DIR, 'venv')

LOG_FILE = os.path.join(CONFIG_DIR, "metadata.json")

config = ConfigReader()
config.load_config(Path(ROOT_DIR, 'config'))

CACHE_MANAGER = CacheManager(Path(ROOT_DIR, 'cache'))
pipeline_cache = CACHE_MANAGER.create("pipeline_cache", "json")

logger = logging.getLogger('Revan')
logging.basicConfig()
logger.setLevel(logging.DEBUG)
