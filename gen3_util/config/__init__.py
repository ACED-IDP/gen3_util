import logging
import os
import pathlib

import requests
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3.index import Gen3Index
from pydantic import BaseModel


def gen_client_ini_path() -> pathlib.Path:
    """Return path to gen3-client ini file. See https://bit.ly/3NbKGi4"""
    return pathlib.Path(pathlib.Path.home() / ".gen3" / "gen3_client_config.ini")


def _get_gen3_client_key(path: pathlib.Path, profile: str = None) -> str:
    """Read gen3-client ini file, return api_key for profile."""

    from gen3_util import read_ini

    gen3_util_ini = read_ini(path)

    if not profile and len(gen3_util_ini.sections()) == 1:
        # default to first section if only one section
        profile = gen3_util_ini.sections()[0]
    if not profile:
        # default to default section if no profile specified
        profile = gen3_util_ini.default_section
    for section in gen3_util_ini.sections():
        if section == profile:
            return gen3_util_ini[section]['api_key']
    raise ValueError(f"no profile '{profile}' found in {path}, specify one of {gen3_util_ini.sections()}, optionally set environmental variable: GEN3_UTIL_PROFILE")


def ensure_auth(refresh_file: [pathlib.Path, str] = None, validate: bool = False, profile: str = None) -> Gen3Auth:
    """Confirm connection to Gen3 using their conventions.

    Args:
        refresh_file (pathlib.Path): The file containing the downloaded JSON web token.
        validate: check the connection by getting a new token
        profile: gen3-client profile

    """

    try:
        if refresh_file:
            if isinstance(refresh_file, str):
                refresh_file = pathlib.Path(refresh_file)
            auth = Gen3Auth(refresh_file=refresh_file.name)
        elif 'ACCESS_TOKEN' in os.environ:
            auth = Gen3Auth(refresh_file=f"accesstoken:///{os.getenv('ACCESS_TOKEN')}")
        elif gen_client_ini_path().exists():
            # https://github.com/uc-cdis/gen3sdk-python/blob/master/gen3/auth.py#L190-L191
            auth = Gen3Auth(refresh_token={
                'api_key': _get_gen3_client_key(gen_client_ini_path(), profile=profile),
            })
        else:
            auth = Gen3Auth()

        if validate:
            api_key = auth.refresh_access_token()
            assert api_key, "refresh_access_token failed"

    except (requests.exceptions.ConnectionError, AssertionError) as e:
        msg = ("Could not get access. "
               "See https://bit.ly/3NbKGi4, or, "
               "store the file in ~/.gen3/credentials.json or specify location with env GEN3_API_KEY "
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

    profile: str = None
    """The name of the gen3-client profile.

    See https://bit.ly/3NbKGi4"""


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


def gen3_services(config: Config) -> tuple[Gen3File, Gen3Index, dict, Gen3Auth]:
    """Create Gen3 Services."""
    auth = ensure_auth(profile=config.gen3.profile)
    file_client = Gen3File(auth_provider=auth)
    index_client = Gen3Index(auth_provider=auth)
    user = auth.curl('/user/user').json()
    return file_client, index_client, user, auth
