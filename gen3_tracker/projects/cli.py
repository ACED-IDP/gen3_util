import sys

import click

from gen3_tracker.common import CLIOutput
from gen3_tracker.config import Config, ensure_auth
from gen3_tracker.gen3.buckets import get_buckets
from gen3_tracker.projects.lister import ls
from gen3_tracker.projects.remover import empty
from gen3_tracker import NaturalOrderGroup, ENV_VARIABLE_PREFIX


@click.group(name='projects', cls=NaturalOrderGroup)
@click.pass_obj
def project_group(config: Config):
    """Manage Gen3 projects."""
    pass


@project_group.command(name="ls")
@click.option('--verbose', default=False, show_default=True, is_flag=True,
              help="List all project details")
@click.pass_obj
def project_ls(config: Config, verbose: bool):
    """List all projects user has access to."""
    with CLIOutput(config=config) as output:
        try:
            auth = ensure_auth(config=config)
            output.update(ls(config, auth=auth, full=verbose))
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e


@project_group.command(name="empty")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--confirm', default=None, show_default=True,
              help="Enter 'empty' to confirm", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_obj
def project_empty(config: Config, project_id: str, confirm: str):
    """Empty all metadata (graph, flat) for a project."""
    with CLIOutput(config=config) as output:
        try:
            assert confirm == 'empty', "Please confirm by entering --confirm empty"
            if not project_id:
                project_id = config.gen3.project_id
                click.secho(f"No project_id provided, using current project {project_id}", fg='yellow',
                            file=sys.stderr)
            _ = empty(config, project_id)
            _['msg'] = f"Emptied {project_id}"
            output.update(_)
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e


@project_group.command(name="bucket")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_obj
def project_bucket(config: Config, project_id: str):
    """Show project bucket."""
    if not project_id:
        project_id = config.gen3.project_id
        click.secho(f"No project_id provided, using current project {project_id}", fg='yellow', file=sys.stderr)
    with CLIOutput(config=config) as output:
        try:
            program, project = project_id.split('-')
            buckets = get_buckets(config=config)
            for k, v in buckets['S3_BUCKETS'].items():
                assert 'programs' in v, f"no configured programs in fence buckets {v} {buckets}"
                if program in v['programs']:
                    output.update({k: v})
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e
