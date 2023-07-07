import logging
import pathlib

import requests
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3.index import Gen3Index
from pydantic import BaseModel


def ensure_auth(refresh_file: [pathlib.Path, str] = None, validate: bool = False) -> Gen3Auth:
    """Confirm connection to Gen3 using their conventions.

    Args:
        refresh_file (pathlib.Path): The file containing the downloaded JSON web token.
        validate: check the connection by getting a new token

    """

    try:
        if refresh_file:
            if isinstance(refresh_file, str):
                refresh_file = pathlib.Path(refresh_file)
            auth = Gen3Auth(refresh_file=refresh_file.name)
        else:
            auth = Gen3Auth()

        if validate:
            api_key = auth.refresh_access_token()
            assert api_key, "refresh_access_token failed"

    except (requests.exceptions.ConnectionError, AssertionError) as e:
        msg = ("Could not get access. "
               "See https://uc-cdis.github.io/gen3-user-doc/appendices/api-gen3/#credentials-to-query-the-api. "
               "Store the file in ~/.gen3/credentials.json or specify location with env GEN3_API_KEY "
               f"{e}")

        logging.getLogger(__name__).error(msg)
        raise AssertionError(msg)

    return auth


class LogConfig(BaseModel):
    format: str
    """https://docs.python.org/3/library/logging.html#logging.Formatter"""
    level: str
    """https://docs.python.org/3/library/logging.html#logging-levels"""


class OutputConfig(BaseModel):
    format: str = "text"
    """write to stdout with this format"""


class _DataclassConfig:
    """Pydantic dataclass configuration

    See https://docs.pydantic.dev/latest/usage/model_config/#options"""
    arbitrary_types_allowed = True


class Gen3Config(BaseModel):

    refresh_file: str = None
    """The file containing the downloaded JSON web token.

    See https://uc-cdis.github.io/gen3sdk-python/_build/html/auth.html#gen3-auth-helper"""


class Config(BaseModel):
    log: LogConfig = LogConfig(
        format='%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s',
        level='INFO'
    )
    """logging setup"""
    output: OutputConfig = OutputConfig(format='yaml')
    """output setup"""
    gen3: Gen3Config = Gen3Config()
    """gen3 setup"""
    state_dir: pathlib.Path = pathlib.Path('~/.gen3/gen3-util-state').expanduser()
    """retry state for file transfer"""


def gen3_services(config: Config) -> tuple[Gen3File, Gen3Index, dict]:
    """Create Gen3 Services."""
    auth = ensure_auth(config.gen3.refresh_file)
    file_client = Gen3File(auth_provider=auth)
    index_client = Gen3Index(auth_provider=auth)
    user = auth.curl('/user/user').json()
    return file_client, index_client, user
