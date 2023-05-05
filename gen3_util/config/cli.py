import logging

import click

from gen3_util.cli.common import NaturalOrderGroup
from gen3_util.config.config import Config
from gen3_util.util import print_formatted


@click.group(name='config', cls=NaturalOrderGroup)
@click.pass_obj
def config_group(config):
    """Configure this utility."""
    pass


@config_group.command(name="ls")
@click.pass_obj
def config_ls(config: Config):
    """Show defaults."""
    print_formatted(config, config)

    logging.getLogger(__name__).debug(config)
