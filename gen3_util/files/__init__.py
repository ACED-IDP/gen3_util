import pathlib
from typing import Dict
import yaml


def read_yaml(path: pathlib.Path) -> Dict:
    """Read a yaml file."""
    with open(path, "r") as fp:
        return yaml.safe_load(fp.read())
