import pathlib
from typing import Dict
import yaml


def read_yaml(path: pathlib.Path) -> Dict:
    """Read a yaml file."""
    with open(path, "r") as fp:
        return yaml.safe_load(fp.read())


def _is_upload(from_, to_) -> bool:
    """Do the parameters describe an upload?"""
    raise NotImplementedError("TODO")
