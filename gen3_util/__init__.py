import sys
from datetime import timezone, datetime, timedelta
from typing import Union

import jwt

from gen3_util.config import config, gen_client_ini_path

default_config = config.default()


def monkey_patch_url_validate():
    # monkey patch to allow file: urls
    import fhir.resources.fhirtypes
    from pydantic import FileUrl

    original_url_validate = fhir.resources.fhirtypes.Url.validate

    @classmethod
    def better_url_validate(cls, value: str, field: "ModelField", config: "BaseConfig") -> Union["AnyUrl", str]:    # noqa
        """Allow file: urls. see https://github.com/pydantic/pydantic/issues/1983
        bugfix: addresses issue introduced with `fhir.resources`==7.0.1
        """
        if value.startswith("file:"):
            return FileUrl.validate(value, field, config)
        value = original_url_validate(value, field, config)
        return value

    fhir.resources.fhirtypes.Url.validate = better_url_validate


def read_ini(path: str):
    """Read ini file."""
    import configparser
    import pathlib

    path = pathlib.Path(path)
    assert path.is_file(), f"{path} is not a file"
    _ = configparser.ConfigParser()
    _.read(path)
    return _


def gen3_client_profile(endpoint: str, path: str = gen_client_ini_path().absolute(), expiration_threshold_days: int = 10) -> str:
    """Read gen3-client ini file, return profile name or none if endpoint not found."""
    gen3_util_ini = read_ini(path)
    matching_sections = []
    for section in gen3_util_ini.sections():
        if gen3_util_ini[section]['api_endpoint'] == endpoint:
            matching_sections.append(section)
            api_key = gen3_util_ini[section]['api_key']
            key = jwt.decode(api_key, options={"verify_signature": False})

            now = datetime.now(tz=timezone.utc).timestamp()
            assert key['exp'] > now, f"key expired {key['exp']} < {now}"
            assert key['iat'] < now, f"key not yet valid {key['iat']} > {now}"
            delta = timedelta(seconds=key['exp'] - now)
            expiration = datetime.fromtimestamp(key['exp'], tz=timezone.utc)
            if delta.days < expiration_threshold_days:
                print(f"WARNING, {section}: Key will expire in {delta.days} days, on {expiration}", file=sys.stderr)
    assert len(matching_sections) <= 1, f"Found multiple profiles for {endpoint}: {matching_sections}"
    return matching_sections[0]


# main
monkey_patch_url_validate()
