import logging
import os
import pathlib
import sys
from datetime import datetime, timezone, timedelta

import jwt
import importlib.resources as pkg_resources
import requests
import yaml
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3.index import Gen3Index

import gen3_util
from gen3_util import Config
from gen3_util.common import read_yaml

def gen_client_ini_path() -> pathlib.Path:
    """Return path to gen3-client ini file. See https://bit.ly/3NbKGi4"""
    return pathlib.Path(pathlib.Path.home() / ".gen3" / "gen3_client_config.ini")


def gen3_client_profile(endpoint: str, path: str = gen_client_ini_path().absolute(), expiration_threshold_days: int = 10) -> str:
    """Read gen3-client ini file, return profile name or none if endpoint not found."""
    gen3_util_ini = read_ini(path)
    matching_sections = []
    for section in gen3_util_ini.sections():
        if gen3_util_ini[section]['api_endpoint'] == endpoint:
            matching_sections.append(section)
            api_key = gen3_util_ini[section]['api_key']
    assert len(matching_sections) <= 1, f"Found multiple profiles for {endpoint}: {matching_sections}"
    return matching_sections[0]


def read_ini(path: str):
    """Read ini file."""
    import configparser
    import pathlib

    path = pathlib.Path(path)
    assert path.is_file(), f"{path} is not a file"
    _ = configparser.ConfigParser()
    _.read(path)
    return _


def key_expired_msg(api_key, expiration_threshold_days, key_name):
    """Confirm that api_key is not expired."""
    key = jwt.decode(api_key, options={"verify_signature": False})
    now = datetime.now(tz=timezone.utc).timestamp()
    msg = 'OK, key is valid'
    exp_str = datetime.fromtimestamp(key['exp'], tz=timezone.utc).isoformat()
    iat_str = datetime.fromtimestamp(key['iat'], tz=timezone.utc).isoformat()
    now_str = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
    if key['exp'] < now:
        msg = f"ERROR key for {key_name} expired {exp_str} < {now_str}"
    if key['iat'] > now:
        msg = f"ERROR key for {key_name} not yet valid {iat_str} > {now_str}"
    delta = timedelta(seconds=key['exp'] - now)
    if 0 < delta.days < expiration_threshold_days:
        msg = f"WARNING {key_name}: Key will expire in {delta.days} days, on {exp_str}"
    return msg


def _get_gen3_client_key(path: pathlib.Path, profile: str = None) -> str:
    """Read gen3-client ini file, return api_key for profile."""

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
            key = _get_gen3_client_key(gen_client_ini_path(), profile=profile)
            msg = key_expired_msg(key, key_name=profile, expiration_threshold_days=10)
            if 'ERROR' in msg:
                raise ValueError(msg.replace('ERROR', ''))  # remove ERROR prefix
            if 'WARNING' in msg:
                print(msg, file=sys.stderr)
            auth = Gen3Auth(refresh_token={
                'api_key': key,
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


def gen3_services(config: Config) -> tuple[Gen3File, Gen3Index, dict, Gen3Auth]:
    """Create Gen3 Services."""
    auth = ensure_auth(profile=config.gen3.profile)
    file_client = Gen3File(auth_provider=auth)
    index_client = Gen3Index(auth_provider=auth)
    user = auth.curl('/user/user').json()
    return file_client, index_client, user, auth


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
