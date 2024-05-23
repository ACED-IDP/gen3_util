import json
import pathlib
import subprocess
import sys
import typing
import uuid
from collections import OrderedDict
from typing import Union, Optional

import click
import pydantic
from click import Context, Command
from pydantic import BaseModel, field_validator


ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-idp.org')
ENV_VARIABLE_PREFIX = 'G3T_'

FILE_TRANSFER_METHODS = {
    'gen3': 'gen3-client to/from local',
    'no-bucket': 'indexd only, symlink to/from local',
    's3': '(admin) s3 to/from local',
    's3-map': '(admin) s3 index only external s3',
}


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
            _ = FileUrl(value)
            return value
            # return FileUrl.validate(value, field, config)
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
    """The name of the gen3-client profile in use. See https://bit.ly/3NbKGi4"""

    version: Optional[str] = None
    """The version of gen3-client in use."""

    project_id: str = None
    """The program-project."""

    @field_validator('project_id')
    def check_project_id(cls, v):
        if v.count('-') != 1:
            raise ValueError('project_id must contain exactly one "-"')
        return v

    @property
    def program(self) -> str:
        if not self.project_id:
            return None
        return self.project_id.split('-')[0]

    @property
    def project(self) -> str:
        if not self.project_id:
            return None
        return self.project_id.split('-')[1]

    @property
    def authz(self) -> str:
        if not self.project_id:
            return None
        return f'/programs/{self.program}/projects/{self.project}'


class Config(BaseModel):
    log: LogConfig = LogConfig(
        format='[%(asctime)s] — [%(levelname)s] — %(name)s — %(message)s',
        level='INFO'
    )
    """logging setup"""
    output: OutputConfig = OutputConfig(format='yaml')
    """output setup"""
    gen3: Gen3Config = Gen3Config()
    """gen3 setup"""
    work_dir: pathlib.Path = None
    """temporary files"""
    no_config_found: bool = False
    """Is this default config used because none found in cwd or parents?"""
    debug: bool = False
    """Enable debug mode, lots of logs."""
    dry_run: bool = False
    """Print the commands that would be executed, but do not execute them."""

    def model_dump(self):
        """Dump the config model.

         temporary until we switch to pydantic2
        """
        _ = json.loads(self.model_dump_json())
        del _['no_config_found']
        return _

    def commit_dir(self):
        """Return the path to the commits' directory."""
        return self.state_dir / self.gen3.project_id / 'commits'


# cli helpers -------------------------------------------------------------------
class NaturalOrderGroup(click.Group):
    """Allow listing Commands in order of appearance, with common parameters."""
    # see https://github.com/pallets/click/issues/513
    def __init__(self, name=None, commands=None, **attrs):
        if commands is None:
            commands = OrderedDict()
        elif not isinstance(commands, OrderedDict):
            commands = OrderedDict(commands)
        click.Group.__init__(self, name=name,
                             commands=commands,
                             **attrs)

    def list_commands(self, ctx):
        # print('list_commands', self.commands.keys())
        return self.commands.keys()

    def resolve_command(
        self, ctx: Context, args: typing.List[str]
    ) -> typing.Tuple[typing.Optional[str], typing.Optional[Command], typing.List[str]]:
        # print('resolve_command', args)
        try:
            return super().resolve_command(ctx, args)
        except Exception as e:
            if 'No such command' in str(e):
                # delegate to git
                try:
                    from gen3_tracker.git import run_command
                    result = run_command(f'git {" ".join(args)}', dry_run=False, no_capture=True)
                    sys.exit(result.return_code)
                    # os._exit(result.return_code)  # noqa
                except subprocess.CalledProcessError as e2:
                    # os._exit(e2.returncode)  # noqa
                    sys.exit(e2.returncode)

                # # suggest git prompt
                # from g3t.common import ERROR_COLOR
                # click.secho(f'Command not found: `g3t {" ".join(args)}`, did you mean: `git {" ".join(args)}` ?',
                #             fg=ERROR_COLOR, file=sys.stderr)
                # os._exit(1)  # noqa

            raise e


# main
monkey_patch_url_validate()

# default initializers for path
pydantic.v1.json.ENCODERS_BY_TYPE[pathlib.PosixPath] = str
pydantic.v1.json.ENCODERS_BY_TYPE[pathlib.WindowsPath] = str
pydantic.v1.json.ENCODERS_BY_TYPE[pathlib.Path] = str
