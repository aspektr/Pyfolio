import yaml


class Config:
    def __init__(self, path="configs/config.yaml"):
        self.path = path

    def load(self):
        """Load config file
        :return: dict {'parameter_name': value}
        """
        with open(self.path, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        return cfg
