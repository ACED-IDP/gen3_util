
import logging
import sys

import pathlib
import subprocess
from importlib.metadata import version as pkg_version

import click

import gen3_util
from gen3_util.access.cli import access_group
from gen3_util.buckets.cli import bucket_group
from gen3_util.cli import StdNaturalOrderGroup, CLIOutput
from gen3_util.cli.cloner import clone
from gen3_util.cli.initializer import initialize_project
from gen3_util.common import print_formatted, LEGACY_PROJECT_DIR
from gen3_util.config import Config, ensure_auth, gen3_client_profiles
from gen3_util.config.cli import config_group
from gen3_util.files.cli import file_group
from gen3_util.jobs.cli import job_group
from gen3_util.meta.cli import meta_group
from gen3_util.projects.cli import project_group
from gen3_util.users.cli import users_group


@click.group(cls=StdNaturalOrderGroup)
@click.pass_context
def cli(ctx, config, output_format, profile, state_dir):
    """Gen3 Management Utilities"""

    config__ = gen3_util.config.default()
    logging.basicConfig(format=config__.log.format, level=config__.log.level, stream=sys.stderr)

    if config:
        config__ = gen3_util.config.custom(config)

    if output_format:
        config__.output.format = output_format

    config__.gen3.profiles = gen3_client_profiles()

    if profile:
        if profile not in config__.gen3.profiles:
            click.secho(f"Profile {profile} not found. Current profiles are: {config__.gen3.profiles}", fg='red')
            exit(1)
        config__.gen3.profile = profile

    if not config__.state_dir:
        state_dir = LEGACY_PROJECT_DIR / 'gen3_util'

    if state_dir:
        _ = pathlib.Path(state_dir).expanduser()
        _.mkdir(parents=True, exist_ok=True)
        config__.state_dir = _

    # ensure that ctx.obj exists
    ctx.obj = config__


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
    _ = pkg_version('gen3-util')
    print_formatted(config, {'version': _})


@cli.command(name='init')
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.pass_obj
def init_cli(config, project_id: str):
    """Create project, both locally and on remote."""
    with (CLIOutput(config=config) as output):
        try:
            _check_parameters(config, project_id)

            logs = initialize_project(config, project_id)

            output.update({'msg': 'Initialized empty repository', 'logs': logs})
        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


def _check_parameters(config, project_id):
    """Common parameter checks."""
    if not project_id:
        raise AssertionError("project_id is required")
    if not project_id.count('-') == 1:
        raise AssertionError("project_id must be of the form program-project")
    if not config.gen3.profile:
        raise AssertionError("No profile specified.")


@cli.command(name='clone')
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option(
    '--data_type', default='all',
    type=click.Choice(['meta', 'files', 'all'], case_sensitive=False),
    required=False, show_default=True,
    help="Clone meta and/or files from remote."
)
@click.pass_obj
def clone_cli(config: Config, project_id: str, data_type: str):
    """Clone meta and files from remote."""
    with CLIOutput(config=config) as output:

        try:
            _check_parameters(config, project_id)
            logs = clone(config, project_id, data_type)
            output.update({'msg': f'Cloned repository {project_id}', 'logs': logs})

        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.command(name="ping")
@click.pass_obj
def ping(config: Config):
    """Verify gen3-client and test connectivity."""
    with CLIOutput(config=config) as output:
        msgs = []
        ok = True
        cmd = "gen3-client --version".split()
        gen3_client_installed = subprocess.run(cmd, capture_output=True)
        if gen3_client_installed.returncode != 0:
            msgs.append("gen3-client not installed")
            ok = False

        gen_client_ini_file = gen3_util.config.gen_client_ini_path()
        auth = None
        if not gen_client_ini_file.exists():
            msgs.append("not configured")
            ok = False
        else:
            try:
                auth = ensure_auth(profile=config.gen3.profile, validate=True)
                msgs.append(f"Connected using profile:{config.gen3.profile}")
            except (AssertionError, ValueError) as e:
                msgs.append(str(e))
                ok = False

        if ok:
            _ = "Configuration OK: "
        else:
            _ = "Configuration ERROR: "
            output.exit_code = 1

        _ = {'msg': _ + ', '.join(msgs)}
        if auth:
            _['endpoint'] = auth.endpoint
            _['username'] = auth.curl('/user/user').json()['username']
        output.update(_)


if __name__ == '__main__':
    cli()
