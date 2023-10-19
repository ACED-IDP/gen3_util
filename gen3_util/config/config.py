import importlib.resources as pkg_resources
import pathlib
import sys

import yaml

import gen3_util
from gen3_util.config import Config
from gen3_util.common import read_yaml


def default():
    """Load config from installed package."""
    if sys.version_info[:3] <= (3, 9):
        return Config(**yaml.safe_load(pkg_resources.open_text(gen3_util, 'config.yaml').read()))
    else:
        # https://docs.python.org/3.11/library/importlib.resources.html#importlib.resources.open_text
        return Config(**yaml.safe_load(pkg_resources.files(gen3_util).joinpath('config.yaml').open().read()))


def custom(path: [str, pathlib.Path]):
    """Load user specified config."""
    if isinstance(path, str):
        path = pathlib.Path(path)

    return Config(**read_yaml(path))
