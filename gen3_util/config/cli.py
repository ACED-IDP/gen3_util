
import click

from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.config.config import Config


@click.group(name='config', cls=NaturalOrderGroup)
@click.pass_obj
def config_group(config):
    """Configure this utility."""
    pass


@config_group.command(name="ls")
@click.pass_obj
def config_ls(config: Config):
    """Show defaults."""
    with CLIOutput(config) as output:
        output.update(config.dict())
