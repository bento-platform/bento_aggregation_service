from importlib import metadata

__all__ = ["name", "__version__"]

name = __package__
__version__ = metadata.version(__package__)
