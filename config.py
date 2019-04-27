import yaml
import log


class Config:
    def __init__(self, path="configs/config.yaml"):
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
            self.logger.warning("Config file %s hasn't been found" % self.path)
            cfg = None

        return cfg
