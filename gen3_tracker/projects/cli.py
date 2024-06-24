import sys

import click

from gen3_tracker.collaborator.access.submitter import ensure_program_project
from gen3_tracker.common import CLIOutput, assert_config, ERROR_COLOR
from gen3_tracker.config import Config, ensure_auth
from gen3_tracker.gen3.buckets import get_buckets
from gen3_tracker.projects.lister import ls
from gen3_tracker.projects.remover import rm, empty
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


@project_group.command(name="create")
@click.option('--all', 'all_projects', default=False, show_default=True, is_flag=True,
              help="Create all projects")
@click.argument('resource', default=None, required=False)
@click.pass_obj
def project_create(config: Config, all_projects: bool, resource: str):
    """Create project(s).

    \b
    RESOURCE: /programs/<program>/projects/<project>
    """

    with CLIOutput(config=config) as output:
        try:
            auth = ensure_auth(config=config)
            assert auth, "auth is required"

            project_ids = []
            if all_projects:
                resources = ls(config, auth=auth, full=True)
                for k, v in resources.projects.items():
                    if not v.exists:
                        _ = k.split('/')
                        assert len(_) == 5, f"Invalid resource: >{k}< {len(_)}"
                        project_ids.append(f"{_[2]}-{_[4]}")
            else:
                if not resource:
                    resource = config.gen3.authz
                    if config.debug:
                        click.secho(f"No resource provided, using current project {resource}", fg='yellow',
                                    file=sys.stderr)
                _ = resource.split('/')
                assert len(_) == 5, f"Invalid resource: >{_}< {len(_)}"
                project_ids.append(f"{_[2]}-{_[4]}")

            assert len(project_ids) > 0, "No projects found"
            submitter_msgs = []
            for policy_id in project_ids:
                if config.debug:
                    click.secho(f"Creating {policy_id}", fg='yellow')
                submitter_msgs.append(ensure_program_project(config, policy_id, auth=auth))
            output.update({'msg': f"OK created {resource}"})
        except Exception as e:
            click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
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


@project_group.command(name="rm")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_obj
def project_rm(config: Config, project_id: str):
    """Remove empty project."""
    with CLIOutput(config=config) as output:
        try:
            output.update(rm(config, project_id))
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
