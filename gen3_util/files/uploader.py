import pathlib
from urllib.parse import urlparse

from gen3_util.util import print_formatted
from gen3_util.config import Config


def _validate_parameters(from_, to_):
    from_ = pathlib.Path(from_)
    assert from_.parent.is_dir(), f"{from_.parent} is not a directory"
    url = urlparse(to_)
    assert url.scheme, f"{to_} does not appear to be a url"


def cp(config: Config, from_: str, to_: str):
    """Copy files to bucket"""
    _validate_parameters(from_, to_)
    print_formatted(config, {'msg': 'file upload progress goes here'})
