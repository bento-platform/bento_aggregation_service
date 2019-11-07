import os

from chord_lib.utils import get_own_version
from pathlib import Path

name = "chord_federation"
__version__ = get_own_version(os.path.join(Path(os.path.dirname(os.path.realpath(__file__))).parent, "setup.py"), name)
