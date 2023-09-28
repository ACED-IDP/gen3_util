import logging
import pathlib

import click
import pkg_resources  # part of setuptools

import gen3_util
from gen3_util.access.cli import access_group
from gen3_util.cli import StdNaturalOrderGroup, CLIOutput
from gen3_util.config import Config, ensure_auth
from gen3_util.config.cli import config_group
from gen3_util.files.cli import file_group
from gen3_util.meta.cli import meta_group
from gen3_util.projects.cli import project_group
from gen3_util.buckets.cli import bucket_group
from gen3_util.jobs.cli import job_group
from gen3_util.common import print_formatted
from gen3_util.users.cli import users_group


@click.group(cls=StdNaturalOrderGroup)
@click.pass_context
def cli(ctx, config, output_format, cred, state_dir):
    """Gen3 Management Utilities"""

    config__ = gen3_util.default_config
    logging.basicConfig(format=config__.log.format, level=config__.log.level)

    if config:
        config__ = gen3_util.config.custom(config)

    if output_format:
        config__.output.format = output_format

    if cred:
        config__.gen3.refresh_file = pathlib.Path(cred).expanduser()

    if state_dir:
        _ = pathlib.Path(state_dir).expanduser()
        _.mkdir(parents=True, exist_ok=True)
        config__.state_dir = _

    # ensure that ctx.obj exists
    ctx.obj = config__
    logging.getLogger(__name__).debug(("config", ctx.obj))


cli.add_command(project_group)
cli.add_command(bucket_group)
cli.add_command(meta_group)
cli.add_command(file_group)
cli.add_command(access_group)
cli.add_command(config_group)
cli.add_command(job_group)
cli.add_command(users_group)


@cli.command(name='version')
@click.pass_obj
def version(config):
    """Print version"""
    _ = pkg_resources.require("gen3_util")[0].version
    print_formatted(config, {'version': _})


@cli.command(name="ping")
@click.pass_obj
def ping(config: Config):
    """Test connectivity to Gen3 endpoint."""
    with CLIOutput(config=config) as output:
        auth = ensure_auth(config.gen3.refresh_file, validate=True)
        output.update({'endpoint': auth.endpoint, 'username': auth.curl('/user/user').json()['username']})


if __name__ == '__main__':
    cli()
