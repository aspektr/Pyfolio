import log
from config import Config
import os


class Base:
    def __init__(self, name):
        # Prepare logger
        log.setup()
        self.logger = log.logging.getLogger(name)

        # Read config
        fname = name + '.yaml'
        self.config = Config(path="config/" + fname).load()
        self.logger.debug("[%u] Config is loaded " % os.getpid())
        self.logger.debug("[%u] Config is %s" %
                          (os.getpid(), self.config))
