from typing import Union
from gen3_util.config import config
import pathlib

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


def gen_client_ini_path():
    """Return path to gen3-client ini file."""
    return pathlib.Path(pathlib.Path.home() / ".gen3" / "gen3_client_config.ini")


def gen3_client_profile(endpoint: str, path: str = gen_client_ini_path().absolute()):
    """Read gen3-client ini file, return profile name or none if endpoint not found."""
    gen3_util_ini = read_ini(path)
    for section in gen3_util_ini.sections():
        if gen3_util_ini[section]['api_endpoint'] == endpoint:
            return section
    return None


# main
monkey_patch_url_validate()
