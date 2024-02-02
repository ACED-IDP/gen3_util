import logging

from gen3_util.config import Config
from gen3_util.common import print_formatted


import click

ENV_VARIABLE_PREFIX = 'G3T_'


def _common_options(self):
    """Insert common commands into Group or Command."""
    assert len(self.params) == 0

    self.params.insert(0,
                       click.core.Option(('--format', 'output_format'),
                                         envvar=f"{ENV_VARIABLE_PREFIX}FORMAT",
                                         default='yaml',
                                         show_default=True,
                                         type=click.Choice(['yaml', 'json', 'text'], case_sensitive=False),
                                         help=f'Result format. {ENV_VARIABLE_PREFIX}FORMAT'))

    # use 'cred', the same name as used in gen3-client data utility
    self.params.insert(1,
                       click.core.Option(('--profile', 'profile'),
                                         envvar=f"{ENV_VARIABLE_PREFIX}PROFILE",
                                         default=None,
                                         show_default=True,
                                         help=f'Connection name. {ENV_VARIABLE_PREFIX}PROFILE See https://bit.ly/3NbKGi4'))

    self.params.insert(2,
                       click.core.Option(('--version', 'version'),
                                         is_flag=True
                                         ))


class StdCommand(click.Command):
    """Allow common parameters.

    See https://stackoverflow.com/a/53875557"""

    def __init__(self, *args, **kwargs):
        """Commands with common parameters"""
        super().__init__(*args, **kwargs)
        _common_options(self)


class NaturalOrderGroup(click.Group):
    """Allow listing Commands in order of appearance, with common parameters.

    See https://github.com/pallets/click/issues/513 """

    def list_commands(self, ctx):
        """Commands in order of appearance"""
        return self.commands.keys()


class StdNaturalOrderGroup(click.Group):
    """Allow listing Commands in order of appearance, with common parameters.

    See https://github.com/pallets/click/issues/513 https://stackoverflow.com/a/53875557"""

    def __init__(self, *args, **kwargs):
        """Commands with common parameters"""
        super().__init__(*args, **kwargs)
        _common_options(self)

    def list_commands(self, ctx):
        """Commands in order of appearance"""
        return self.commands.keys()


class CommandOutput(object):
    """Output object for commands."""
    def __init__(self):
        self.obj = None
        self.exit_code = 0

    def update(self, obj):
        """Update output with obj."""
        self.obj = obj


class CLIOutput:
    """Ensure output, exceptions and exit code are returned to user consistently."""
    def __init__(self, config: Config, exit_on_error: bool = True):
        self.output = CommandOutput()
        self.config = config
        self.exit_on_error = exit_on_error

    def __enter__(self):
        return self.output

    def __exit__(self, exc_type, exc_val, exc_tb):
        rc = 0
        _ = {}
        if self.output.obj is not None:
            if isinstance(self.output.obj, dict):
                _.update(self.output.obj)
            elif isinstance(self.output.obj, list):
                _ = self.output.obj
            elif isinstance(self.output.obj, int):
                _ = {'count': self.output.obj}
            elif hasattr(self.output.obj, 'model_dump'):
                _.update(self.output.obj.model_dump())
            else:
                _.update(self.output.obj.dict())
        rc = self.output.exit_code
        if exc_type is not None:
            if isinstance(self.output.obj, dict):
                _['exception'] = f"{str(exc_val)}"
            elif isinstance(self.output.obj, list):
                _.append(f"{str(exc_val)}")
            else:
                _.update({'exception': f"{str(exc_val)}"})
            rc = 1
            logging.getLogger(__name__).exception(exc_val)
        if isinstance(_, dict) and 'msg' not in _:
            if rc == 1:
                _['msg'] = 'FAIL'
            else:
                _['msg'] = 'OK'
        prune = []
        if isinstance(_, dict):
            for k, v in _.items():
                if not v:
                    prune.append(k)
            for k in prune:
                del _[k]
        print_formatted(self.config, _)
        self.output.exit_code = rc
        if rc != 0 and self.exit_on_error:
            exit(rc)
