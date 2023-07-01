import logging

from gen3_util.config import Config
from gen3_util.common import print_formatted


import click

ENV_VARIABLE_PREFIX = 'GEN3_UTIL'


def _common_options(self):
    """Insert common commands into Group or Command."""
    assert len(self.params) == 0
    self.params.insert(0,
                       click.core.Option(('--config',),
                                         envvar=f"{ENV_VARIABLE_PREFIX}_CONFIG",
                                         default=None,
                                         required=False,
                                         show_default=True,
                                         help=f'Path to config file. {ENV_VARIABLE_PREFIX}_CONFIG'))
    self.params.insert(1,
                       click.core.Option(('--format', 'output_format'),
                                         envvar=f"{ENV_VARIABLE_PREFIX}_FORMAT",
                                         default='yaml',
                                         show_default=True,
                                         type=click.Choice(['yaml', 'json', 'text'], case_sensitive=False),
                                         help=f'Result format. {ENV_VARIABLE_PREFIX}_FORMAT'))

    # use 'cred', the same name as used in gen3-client data utility
    self.params.insert(2,
                       click.core.Option(('--cred', 'cred'),
                                         envvar="GEN3_API_KEY",
                                         default=None,
                                         show_default=True,
                                         help='See https://uc-cdis.github.io/gen3-user-doc/appendices'
                                              '/api-gen3/#credentials-to-query-the-api. GEN3_API_KEY'))

    self.params.insert(3,
                       click.core.Option(('--state_dir', ),
                                         envvar=f"{ENV_VARIABLE_PREFIX}_STATE_DIR",
                                         default='~/.gen3/gen3_util',
                                         show_default=True,
                                         help=f'Directory for file transfer state {ENV_VARIABLE_PREFIX}_STATE_DIR'))


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
            else:
                _.update(self.output.obj.dict())
        if exc_type is not None:
            _['exception'] = f"{str(exc_val)}"
            rc = 1
            logging.getLogger(__name__).exception(exc_val)
        if 'msg' not in _:
            if rc == 1:
                _['msg'] = 'FAIL'
            else:
                _['msg'] = 'OK'
        prune = []
        for k, v in _.items():
            if not v:
                prune.append(k)
        for k in prune:
            del _[k]
        print_formatted(self.config, _)
        self.output.exit_code = rc
        if self.exit_on_error:
            exit(rc)
