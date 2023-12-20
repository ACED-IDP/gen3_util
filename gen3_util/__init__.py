import pathlib
import uuid
from typing import Union

from pydantic import BaseModel
ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')


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


# main
monkey_patch_url_validate()
