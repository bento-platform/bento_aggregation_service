import configparser
import os

__all__ = ["name", "__version__"]

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), "package.cfg"))

name = config["package"]["name"]
__version__ = config["package"]["version"]
