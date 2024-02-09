import click

from gen3_util.access.submitter import ensure_program_project
from gen3_util.config import Config, ensure_auth
from gen3_util.projects.lister import ls
from gen3_util.projects.remover import rm, empty
from gen3_util.repo import CLIOutput, ENV_VARIABLE_PREFIX
from gen3_util.repo import NaturalOrderGroup


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
                assert resource, "resource is required"
                _ = resource.split('/')
                assert len(_) == 5, f"Invalid resource: >{k}< {len(_)}"
                project_ids.append(f"{_[2]}-{_[4]}")

            assert len(project_ids) > 0, "No projects found"
            submitter_msgs = []
            for policy_id in project_ids:
                click.secho(f"Creating {policy_id}", fg='yellow')
                submitter_msgs.append(ensure_program_project(config, policy_id, auth=auth))

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@project_group.command(name="empty")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_obj
def project_empty(config: Config, project_id: str):
    """Empty all metadata (graph, flat) for a project."""
    with CLIOutput(config=config) as output:
        try:
            _ = empty(config, project_id)
            _['msg'] = f"Emptied {project_id}"
            output.update(_)
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


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
