
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
                                         help=f'Path to config file. {ENV_VARIABLE_PREFIX}_CONFIG'))
    self.params.insert(1,
                       click.core.Option(('--format', 'output_format'),
                                         envvar=f"{ENV_VARIABLE_PREFIX}_FORMAT",
                                         default='text',
                                         type=click.Choice(['yaml', 'json', 'text'], case_sensitive=False),
                                         help=f'Result format. {ENV_VARIABLE_PREFIX}_FORMAT'))

    # use 'cred', the same name as used in gen3-client data utility
    self.params.insert(2,
                       click.core.Option(('--cred', 'cred'),
                                         envvar="GEN3_API_KEY",
                                         default=None,
                                         help='See https://uc-cdis.github.io/gen3-user-doc/appendices'
                                              '/api-gen3/#credentials-to-query-the-api. GEN3_API_KEY'))


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
