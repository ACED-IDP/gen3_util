import logging
import sys

import click

import gen3_tracker
from gen3_tracker import NaturalOrderGroup, ENV_VARIABLE_PREFIX
from gen3_tracker.collaborator.cli import collaborator
from gen3_tracker.config import gen3_client_profiles
from gen3_tracker.git.cli import cli as git
from gen3_tracker.meta.cli import meta
from gen3_tracker.projects.cli import project_group as project

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__package__)


@click.group(cls=NaturalOrderGroup)
@click.option('--format', 'output_format',
              envvar=f"{ENV_VARIABLE_PREFIX}FORMAT",
              default='yaml',
              show_default=True,
              type=click.Choice(['yaml', 'json', 'text'], case_sensitive=False),
              help=f'Result format. {ENV_VARIABLE_PREFIX}FORMAT'
              )
@click.option('--profile', 'profile',
              envvar=f"{ENV_VARIABLE_PREFIX}PROFILE",
              default=None,
              show_default=True,
              help=f'Connection name. {ENV_VARIABLE_PREFIX}PROFILE See https://bit.ly/3NbKGi4'
              )
@click.option('--debug', is_flag=True, envvar='G3T_DEBUG', help='Enable debug mode. G3T_DEBUG environment variable can also be used.')
@click.option('--dry-run', is_flag=True, envvar='G3T_DRYRUN', help='Print the commands that would be executed, but do not execute them. G3T_DRYRUN environment variable can also be used.')
@click.version_option()
@click.pass_context
def cli(ctx: click.Context, output_format: str, profile: str, debug: bool, dry_run: bool):
    """A CLI for adding version control to Gen3 projects."""
    config__ = gen3_tracker.config.default()
    logging.basicConfig(format=config__.log.format, level=config__.log.level, stream=sys.stderr)

    if output_format:
        config__.output.format = output_format

    _profiles = gen3_client_profiles()
    is_help = '--help' in sys.argv[1:]

    if profile:
        if profile not in _profiles:
            click.secho(f"Profile {profile} not found.", fg='red')
            exit(1)
        config__.gen3.profile = profile
    elif not config__.gen3.profile and not is_help:
        if not _profiles:
            click.secho("No gen3_client profile found.", fg='red')
            exit(1)
        else:
            if len(_profiles) > 1:
                click.secho(f"WARNING: No --profile specified, found multiple gen3_client profiles: {_profiles}",
                            fg='red')
            else:
                click.secho(f"Using default gen3_client profile {_profiles[0]}", fg='yellow')
                config__.gen3.profile = _profiles[0]

    # ensure that ctx.obj exists
    config__.debug = debug
    config__.dry_run = dry_run

    ctx.obj = config__

    if debug:
        _logger.setLevel(logging.DEBUG)


git.add_command(meta)
git.add_command(collaborator)
git.add_command(project)

for command in git.commands.values():
    cli.add_command(command)


if __name__ == "__main__":
    cli()
