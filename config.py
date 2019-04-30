import yaml
import log
import os


class Config:
    def __init__(self, path="config/config.yaml"):
        self.path = path
        log.setup()
        self.logger = log.logging.getLogger(__name__)

    def load(self):
        """Load config file
        :return: dict {'parameter_name': value}
        """
        try:
            with open(self.path, 'r') as ymlfile:
                cfg = yaml.load(ymlfile)
        except FileNotFoundError:
            self.logger.warning("[%u] Config file %s hasn't been found" %
                                (os.getpid(), self.path))
            self.logger.warning("[%u] Current directory is %s" %
                                (os.getpid(), os.getcwd()))
            cfg = None

        return cfg
