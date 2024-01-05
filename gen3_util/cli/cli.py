import logging
import pathlib
import subprocess
from importlib.metadata import version as pkg_version

import click

import gen3_util
from gen3_util.access.cli import access_group
from gen3_util.access.requestor import add_policies
from gen3_util.buckets.cli import bucket_group
from gen3_util.cli import StdNaturalOrderGroup, CLIOutput
from gen3_util.common import print_formatted, LEGACY_PROJECT_DIR
from gen3_util.config import Config, ensure_auth, gen3_client_profiles, init
from gen3_util.config.cli import config_group
from gen3_util.files.cli import file_group
from gen3_util.jobs.cli import job_group
from gen3_util.meta.cli import meta_group
from gen3_util.projects.cli import project_group
from gen3_util.users.cli import users_group

from gen3_util.projects.lister import ls as project_ls


@click.group(cls=StdNaturalOrderGroup)
@click.pass_context
def cli(ctx, config, output_format, profile, state_dir):
    """Gen3 Management Utilities"""

    config__ = gen3_util.config.default()
    logging.basicConfig(format=config__.log.format, level=config__.log.level)

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
            logs = []

            if not project_id:
                raise AssertionError("project_id is required")

            if not project_id.count('-') == 1:
                raise AssertionError("project_id must be of the form program-project")

            if not config.gen3.profile:
                raise AssertionError("No profile specified.")

            auth = ensure_auth(profile=config.gen3.profile)
            program, project = project_id.split('-')
            projects = project_ls(config, auth=auth)
            existing_project = [_ for _ in projects.projects if _.endswith(project)]
            if len(existing_project) > 0:
                raise AssertionError(f"Project already exists: {existing_project[0]}")

            _ = add_policies(config, project_id, auth=auth)
            policy_msgs = [_.msg, f"See {_.commands}"]

            for _ in init(config, project_id):
                logs.append(_)
            logs.extend(policy_msgs)

            output.update({'msg': 'Initialized empty repository', 'logs': logs})
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
