import logging

import click

import gen3_util
from gen3_util.util import print_formatted
from gen3_util.cli.common import StdNaturalOrderGroup
from gen3_util.files.cli import file_group
from gen3_util.meta.cli import meta_group
from gen3_util.projects.cli import project_group
from gen3_util.config.cli import config_group

import pkg_resources  # part of setuptools


@click.group(cls=StdNaturalOrderGroup, invoke_without_command=True)
@click.pass_context
def cli(ctx, config, output_format):
    """Gen3 Management Utilities"""

    config__ = gen3_util.default_config

    if config:
        config__ = gen3_util.config.custom(config)

    if output_format:
        config__.output.format = output_format

    # print(("cli group stdout", config, config_))
    logging.basicConfig(format=config__.log.format, level=config__.log.level)

    # ensure that ctx.obj exists and is a dict
    ctx.obj = config__
    logging.getLogger(__name__).debug(("config", ctx.obj))

    if ctx.invoked_subcommand is None:
        version = pkg_resources.require("gen3_util")[0].version
        print_formatted(config__, {'msg': f'Version {version}'})


cli.add_command(project_group)
cli.add_command(meta_group)
cli.add_command(file_group)
cli.add_command(config_group)


if __name__ == '__main__':
    cli()
