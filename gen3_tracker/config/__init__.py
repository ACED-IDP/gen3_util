import logging
import os
import pathlib
import sys
from configparser import ConfigParser
from datetime import datetime, timezone, timedelta
from typing import Generator

import click
import jwt
import importlib.resources as pkg_resources
import requests
import yaml
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3.index import Gen3Index

import gen3_tracker
from gen3_tracker import Config
from gen3_tracker.common import read_yaml, PROJECT_DIRECTORIES, PROJECT_DIR, PROJECT_README


def gen_client_ini_path() -> pathlib.Path:
    """Return path to gen3-client ini file. See https://bit.ly/3NbKGi4"""
    return pathlib.Path(pathlib.Path.home() / ".gen3" / "gen3_client_config.ini")


# TODO - unused, deprecate?
def gen3_client_profile(endpoint: str, path: str = gen_client_ini_path().absolute(), expiration_threshold_days: int = 10) -> str:
    """Read gen3-client ini file, return profile name or none if endpoint not found."""
    gen3_util_ini = read_ini(path)
    matching_sections = []
    for section in gen3_util_ini.sections():
        if gen3_util_ini[section]['api_endpoint'] == endpoint:
            matching_sections.append(section)
    assert len(matching_sections) <= 1, f"Found multiple profiles for {endpoint}: {matching_sections}"
    return matching_sections[0]


def gen3_client_profiles(path: str = gen_client_ini_path().absolute()) -> list[str]:
    """Read gen3-client ini file, return list of profiles."""
    gen3_util_ini = read_ini(path)
    return gen3_util_ini.sections()


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


def _get_gen3_client_default_profile(path: pathlib.Path = None, gen3_util_ini: ConfigParser = None) -> str:
    """Read gen3-client ini file, return default (only) profile."""
    if gen3_util_ini is None:
        assert path, "path is required"
        gen3_util_ini = read_ini(path)
    if len(gen3_util_ini.sections()) == 1:
        # default to first section if only one section
        profile = gen3_util_ini.sections()[0]
        return profile
    return None


def _get_gen3_client_key(path: pathlib.Path, profile: str = None) -> str:
    """Read gen3-client ini file, return api_key for profile."""

    gen3_util_ini = read_ini(path)

    if profile:
        for section in gen3_util_ini.sections():
            if section == profile:
                return gen3_util_ini[section]['api_key']
    else:
        profile = _get_gen3_client_default_profile(gen3_util_ini=gen3_util_ini)
        if profile:
            return gen3_util_ini[profile]['api_key']
    click.secho(f"no profile '{profile}' found in {path}, specify one of {gen3_util_ini.sections()}, optionally set environmental variable: GEN3_UTIL_PROFILE", fg='yellow')


def ensure_auth(refresh_file: [pathlib.Path, str] = None, validate: bool = False, config: Config = None) -> Gen3Auth:
    """Confirm connection to Gen3 using their conventions.

    Args:
        refresh_file (pathlib.Path): The file containing the downloaded JSON web token.
        validate: check the connection by getting a new token
        config: Config

    """

    try:
        if refresh_file:
            if isinstance(refresh_file, str):
                refresh_file = pathlib.Path(refresh_file)
            auth = Gen3Auth(refresh_file=refresh_file.name)
        elif 'ACCESS_TOKEN' in os.environ:
            auth = Gen3Auth(refresh_file=f"accesstoken:///{os.getenv('ACCESS_TOKEN')}")
        elif gen_client_ini_path().exists():
            profile = config.gen3.profile
            if not profile:
                # in disconnected mode, or not in project dir
                if config.no_config_found:
                    print("INFO: No config file found in current directory or parents.", file=sys.stderr)
                return None
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
        msg = (f"Could not get access. profile={profile}"
               "See https://bit.ly/3NbKGi4, or, "
               "store the file in ~/.gen3/credentials.json or specify location with env GEN3_API_KEY "
               f"{e}")

        logging.getLogger(__name__).error(msg)
        raise AssertionError(msg)

    return auth


# TODO - unused, deprecate?
def gen3_services(config: Config) -> tuple[Gen3File, Gen3Index, dict, Gen3Auth]:
    """Create Gen3 Services."""
    auth = ensure_auth(config=config)
    assert auth, f"Failed to set auth {config.gen3.profile}"
    file_client = Gen3File(auth_provider=auth)
    index_client = Gen3Index(auth_provider=auth)
    user = auth.curl('/user/user').json()
    return file_client, index_client, user, auth


def search_upwards_for_file(filename):
    """Search in the current directory and all directories above it
    for a file of a particular name.

    Arguments:
    ---------
    filename :: string, the filename to look for.

    Returns
    -------
    pathlib.Path, the location of the first file found or
    None, if none was found
    """
    d = pathlib.Path.cwd()
    root = pathlib.Path(d.root)

    while d != root:
        attempt = d / filename
        if attempt.exists():
            return attempt
        d = d.parent

    return None


def default():
    """Load config from directory or installed package."""

    # in current dir?
    _ = pathlib.Path(PROJECT_DIR) / 'config.yaml'
    if _.exists():
        return Config(**read_yaml(_))

    # look in parents
    parent_dir = search_upwards_for_file(PROJECT_DIR)
    if parent_dir:
        _ = parent_dir / 'config.yaml'
        if _.exists():
            return Config(**read_yaml(_))

    # use default

    # different pkg_resources open for 3.9
    if sys.version_info[:3] <= (3, 9):
        _config = Config(**yaml.safe_load(pkg_resources.open_text(gen3_tracker, 'config.yaml').read()))
    else:
        # https://docs.python.org/3.11/library/importlib.resources.html#importlib.resources.open_text
        _config = Config(**yaml.safe_load(pkg_resources.files(gen3_tracker).joinpath('config.yaml').open().read()))

    # No config file found in directory or parents.
    _config.no_config_found = True
    return _config


def custom(path: [str, pathlib.Path]):
    """Load user specified config."""
    if isinstance(path, str):
        path = pathlib.Path(path)

    return Config(**read_yaml(path))


def init(config: Config, project_id: str) -> Generator[str, None, None]:
    """Create an empty repository, adjust and write config file"""

    logger = logging.getLogger(__name__)
    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."

    existing_dirs = []
    for _ in PROJECT_DIRECTORIES:
        if pathlib.Path(_).exists():
            existing_dirs.append(_)
        pathlib.Path(_).mkdir(exist_ok=True)

    if existing_dirs:
        yield f"Directory already exists {existing_dirs}"
    else:
        yield f"Created project directories {PROJECT_DIRECTORIES}"

    config.gen3.project_id = project_id
    config.work_dir = pathlib.Path(PROJECT_DIR) / 'work'
    config.work_dir.mkdir(parents=True, exist_ok=True)

    work_dir_git_ignore = config.work_dir / ".gitignore"
    if not pathlib.Path(work_dir_git_ignore).exists():
        with open(work_dir_git_ignore, 'w') as f:
            f.write("*\n!README.md\n!.gitignore\n")

    # meta_dir_git_ignore = "META/.gitignore"
    # if not pathlib.Path(meta_dir_git_ignore).exists():
    #     with open(meta_dir_git_ignore, 'w') as f:
    #         f.write("*\n!README.md")

    for readme in [PROJECT_DIR + '/README.md', "META/README.md"]:
        if not pathlib.Path(readme).exists():
            path = pathlib.Path(readme)
            with open(path, 'w') as f:
                f.write(PROJECT_README)

    config_file = pathlib.Path(PROJECT_DIR) / 'config.yaml'
    if not config_file.exists():
        with open(config_file, 'w') as f:
            yaml.dump(config.model_dump(), f)
        yield f"Created project configuration file={config_file} project_id={config.gen3.project_id} profile={config.gen3.profile}"
    else:
        updated_config = yaml.load(open(config_file), Loader=yaml.SafeLoader)
        updated_config = Config(**updated_config)
        updated_config.gen3.project_id = project_id
        updated_config.gen3.profile = config.gen3.profile
        with open(config_file, 'w') as f:
            yaml.dump(updated_config.model_dump(), f)
        yield f"Updated project configuration file={config_file} project_id={config.gen3.project_id} profile={config.gen3.profile}"
