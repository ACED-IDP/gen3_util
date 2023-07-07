import pathlib
from urllib.parse import ParseResult, urlparse

from gen3_util.config import Config
from gen3_util.common import print_formatted


def _validate_parameters(from_, to_) -> (ParseResult, pathlib.Path):
    assert len(urlparse(to_).scheme) == 0, f"{to_} appears to be a url. url to url cp not supported"
    to_ = pathlib.Path(to_)
    assert to_.parent.exists(), f"{to_.parent} is not a directory"
    if '*' in to_.stem:
        assert len(to_.parent.glob(to_.stem)) > 0, f"{to_} does not match any files."
    else:
        assert to_.exists(), f"{to_} does not exist"
    url = urlparse(from_)
    assert url.scheme, f"{from_} does not appear to be a url"
    return from_, to_


def cp(config: Config, from_: str, to_: str):
    """Copy files from bucket to local file system."""
    # from_, to_ = _validate_parameters(from_, to_)
    print_formatted(config, {'msg': 'Please use "gen3 file download-single OBJECT_ID"'})
