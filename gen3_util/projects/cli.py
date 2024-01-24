import click

from gen3_util.config import Config, ensure_auth
from gen3_util.projects.lister import ls
from gen3_util.projects.remover import rm
from gen3_util.repo import CLIOutput, ENV_VARIABLE_PREFIX
from gen3_util.repo import NaturalOrderGroup


@click.group(name='projects', cls=NaturalOrderGroup)
@click.pass_obj
def project_group(config: Config):
    """Manage Gen3 projects."""
    pass


# @project_group.command(name="new")
# @click.option('--project_id', default=None, show_default=True,
#               help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
# @click.pass_obj
# def new_project(config: Config, project_id: str):
#     """Creates project resource with default policies.
#     """
#     with CLIOutput(config=config) as output:
#         auth = ensure_auth(config=config)
#         msgs = validate_project_id(project_id)
#         if not msgs:
#             program, project = project_id.split('-')
#             projects = ls(config, auth=auth)
#             existing_project = [_ for _ in projects.projects if _.endswith(project)]
#             if len(existing_project) > 0:
#                 msgs.append(f"Project already exists: {existing_project[0]}")
#             else:
#                 output.update(add_policies(config, project_id, auth=auth))
#         if msgs:
#             output.update({'msg': ', '.join(msgs)})
#

@project_group.command(name="ls")
@click.option('--full', default=False, show_default=True, is_flag=True,
              help="List all project details")
@click.pass_obj
def project_ls(config: Config, full: bool):
    """List all projects user has access to."""
    auth = ensure_auth(config=config)
    with CLIOutput(config=config) as output:
        output.update(ls(config, auth=auth, full=full))


@project_group.command(name="rm")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_obj
def project_rm(config: Config, project_id: str):
    """Remove project.
    """
    with CLIOutput(config=config) as output:
        output.update(rm(config, project_id))
