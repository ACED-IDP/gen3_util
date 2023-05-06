import importlib.resources as pkg_resources
import pathlib

import yaml

import gen3_util
from gen3_util.config import Config
from gen3_util.common import read_yaml


def default():
    """Load config from installed package."""
    return Config(**yaml.safe_load(pkg_resources.open_text(gen3_util, 'config.yaml').read()))


def custom(path: [str, pathlib.Path]):
    """Load user specified config."""
    if isinstance(path, str):
        path = pathlib.Path(path)

    return Config(**read_yaml(path))
