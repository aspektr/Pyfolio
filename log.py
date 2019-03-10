import os
import logging.config
import yaml


def setup(
        default_path='config/logging.yaml',
        default_level=logging.INFO,
        env_key='LOG_CFG'  # to load the logging configuration from specific path
):
    """Setup logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
