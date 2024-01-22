
import logging
import sys

import pathlib
import subprocess
from datetime import datetime
from importlib.metadata import version as pkg_version

import click
from gen3.auth import Gen3AuthError

import gen3_util
from gen3_util.access.cli import access_group
from gen3_util.buckets.cli import bucket_group
from gen3_util.repo import StdNaturalOrderGroup, CLIOutput, NaturalOrderGroup, ENV_VARIABLE_PREFIX
from gen3_util.repo.cloner import clone
from gen3_util.repo.committer import commit
from gen3_util.repo.initializer import initialize_project_server_side
from gen3_util.repo.puller import pull_files
from gen3_util.repo.pusher import push
from gen3_util.repo.status import status
from gen3_util.common import write_meta_index, PROJECT_DIR
from gen3_util.config import Config, ensure_auth, gen3_client_profiles, init
from gen3_util.config.cli import config_group
from gen3_util.files.cli import file_group, manifest_put_cli
from gen3_util.jobs.cli import job_group
from gen3_util.meta.cli import meta_group
from gen3_util.projects.cli import project_group
from gen3_util.users.cli import users_group


@click.group(cls=StdNaturalOrderGroup, invoke_without_command=True)
@click.pass_context
def cli(ctx, output_format, profile, version):
    """Gen3 Tracker: manage FHIR metadata and files."""
    if version:
        _ = pkg_version('gen3-util')
        click.echo(_)
        ctx.exit()

    config__ = gen3_util.config.default()
    logging.basicConfig(format=config__.log.format, level=config__.log.level, stream=sys.stderr)

    # if config:
    #     config__ = gen3_util.config.custom(config)

    if output_format:
        config__.output.format = output_format

    if profile:
        if profile not in gen3_client_profiles():
            click.secho(f"Profile {profile} not found.", fg='red')
            exit(1)
        config__.gen3.profile = profile

    # if state_dir:
    #     _ = pathlib.Path(state_dir).expanduser()
    #     _.mkdir(parents=True, exist_ok=True)
    #     config__.state_dir = _

    # ensure that ctx.obj exists
    ctx.obj = config__


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
            except Gen3AuthError as e:
                msg = str(e).split(':')[0]
                msgs.append(msg)
                msg2 = str(e).split('<p class="introduction">')[-1]
                msg2 = msg2.split('</p>')[0]
                msgs.append(msg2)
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


@cli.command(name='init')
@click.option('--project_id', default=None, required=False, show_default=True,
              help=f"Gen3 program-project {ENV_VARIABLE_PREFIX}PROJECT_ID", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_obj
def init_cli(config, project_id: str):
    """Create project, both locally and on remote."""
    with (CLIOutput(config=config) as output):
        try:
            _check_parameters(config, project_id)
            logs = []
            # create directories
            for _ in init(config, project_id):
                logs.append(_)

            # request the project get signed
            logs.extend(initialize_project_server_side(config, project_id))

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
        click.secho("No profile set. Continuing in disconnected mode. Use `set profile <profile>`", fg='yellow')


cli.add_command(manifest_put_cli)


@cli.command(name='commit')
@click.argument('metadata_path', type=click.Path(exists=True), default='META', required=False)
@click.option('--message', '-m', default=None, required=True, show_default=True,
              help="Use the given <msg> as the commit message.")
@click.pass_obj
def commit_cli(config: Config, metadata_path: str, message: str):
    """Record changes to the project.

    \b
    METADATA_PATH: directory containing metadata files to be committed. [default: ./META]
    """
    with CLIOutput(config=config) as output:
        try:
            assert config.gen3.project_id, "Not in an initialed project directory."
            project_id = config.gen3.project_id
            metadata_path = pathlib.Path(metadata_path)
            _check_parameters(config, project_id)
            results = commit(config, metadata_path, pathlib.Path().cwd(), message)
            if not results.message:
                results.message = 'Saved committed changes.'
            _ = results.model_dump()
            _['msg'] = results.message
            output.update(_)

        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.command(name='push')
@click.option('--overwrite', default=False, is_flag=True, required=False, show_default=True,
              help="overwrite files records in index")
@click.option('--restricted_project_id', default=None, required=False, show_default=True,
              help="adds additional access control")
@click.pass_obj
def push_cli(config: Config, restricted_project_id: str, overwrite: bool):
    """Submit committed changes to commons."""
    with CLIOutput(config=config) as output:
        try:
            overwrite_files = overwrite_index = overwrite
            output.update(
                push(config, restricted_project_id=restricted_project_id, overwrite_index=overwrite_index,
                     overwrite_files=overwrite_files)
            )
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.command(name="status")
@click.pass_obj
def status_cli(config: Config):
    """Show the working tree status."""
    with CLIOutput(config=config) as output:
        try:
            assert config.gen3.project_id, "Not in an initialed project directory."
            project_id = config.gen3.project_id
            _check_parameters(config, project_id)
            output.update(status(config))

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.command(name='clone')
@click.option('--project_id', default=None, required=False, show_default=True,
              help=f"Gen3 program-project {ENV_VARIABLE_PREFIX}PROJECT_ID", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option(
    '--data_type', default='meta',
    type=click.Choice(['meta', 'files', 'all'], case_sensitive=False),
    required=False, show_default=True,
    help="Clone meta and/or files from remote."
)
@click.pass_obj
def clone_cli(config: Config, project_id: str, data_type: str):
    """Clone meta and files from remote."""

    with CLIOutput(config=config) as output:
        try:
            assert config.gen3.profile, "Disconnected mode. Use --profile"
            _check_parameters(config, project_id)
            print(f"Cloning {project_id}...", file=sys.stderr)
            logs = clone(config, project_id, data_type)
            output.update({'msg': f'Cloned repository {project_id}', 'logs': logs})

        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.command(name="pull")
@click.pass_obj
def pull_cli(config: Config):
    """Download data files."""
    with CLIOutput(config=config) as output:
        try:
            assert config.gen3.project_id, "Not in an initialed project directory."
            project_id = config.gen3.project_id
            _check_parameters(config, project_id)
            now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            manifest_name = f"pull_{project_id}_{now}.manifest.json"
            path = pathlib.Path.cwd()
            original_path = path  # used to make path relative in the log message
            output.update(
                pull_files(
                    config=config,
                    manifest_name=manifest_name,
                    original_path=original_path,
                    path=path,
                    auth=None
                )
            )

        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.command(name="update-index")
@click.pass_obj
def update_index_cli(config: Config):
    """Update the index from the META directory."""
    assert pathlib.Path(PROJECT_DIR).exists(), "Please run from the project root directory."
    with CLIOutput(config=config) as output:
        try:
            write_meta_index(
                index_path=config.state_dir,
                source_path=(pathlib.Path.cwd() / 'META')
            )
            output.update({'msg': 'OK'})
        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@cli.group(name='utilities', cls=NaturalOrderGroup)
@click.pass_obj
def utilities_group(config):
    """Useful utilities."""
    pass


utilities_group.add_command(file_group)
utilities_group.add_command(project_group)
utilities_group.add_command(bucket_group)
utilities_group.add_command(meta_group)
utilities_group.add_command(access_group)
utilities_group.add_command(config_group)
utilities_group.add_command(job_group)
utilities_group.add_command(users_group)


if __name__ == '__main__':
    cli()
