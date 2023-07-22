import click
import yaml

from gen3_util.access import create_request
from gen3_util.access.requestor import ls, cat, update, format_policy
from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.common import validate_project_id, validate_email, to_resource_path, print_formatted
from gen3_util.config import Config


@click.group(name='access', cls=NaturalOrderGroup)
@click.pass_obj
def access_group(config: Config):
    """Manage access requests."""
    pass


@access_group.command(name="touch")
@click.argument('user_name')
@click.argument('project_id')
@click.option('--roles', show_default=True, default=None, help='Add comma-delimited role permissions to the access request, ex: --roles "storage_writer,file_uploader"')
@click.pass_obj
def access_touch(config: Config,  user_name: str, project_id: str, roles: str):
    """Create a request for read access.

    \b
    USER_NAME (str): user's email
    PROJECT_ID or RESOURCE_PATH: <program-name>-<project-name> or /resource/path

    """
    msgs = validate_email(user_name)
    assert msgs == [], f"Invalid email address: {user_name} {msgs}"

    msgs = validate_project_id(project_id)
    # assert msgs == [], f"Invalid project id: {project_id} {msgs}"

    if len(msgs) == 0:
        resource_path = to_resource_path(project_id)
    else:
        resource_path = project_id  # assume resource path passed

    assert user_name, "required"

    assert (project_id or resource_path), "required"

    resource_path = to_resource_path(project_id)

    request = {"username": user_name, "resource_path": resource_path}
    if roles is not None:
        roles = list(map(str, roles.split(',')))
        request.update({"role_ids": roles})

    with CLIOutput(config=config) as output:
        output.update(create_request(config=config, request=request))


@access_group.command(name="cp")
@click.argument('path')
@click.option('--user_name', required=False, help='User email address to apply to all policies, defaults to current user')
@click.option('--project_id', required=False, help='Project ID to apply to all policies in template, ex: --project_id "program-project"')
@click.pass_obj
def access_cp(config: Config,  path: str, user_name: str, project_id: str):
    """Read YAML file and create a request for access.

    File should conform to the following format:
    https://github.com/uc-cdis/requestor/blob/master/docs/openapi.yaml#L26

    \b
    PATH (str): yaml file

    """
    create_request_input = yaml.safe_load((open(path, 'r')))
    responses = []
    for policy in create_request_input['policies']:
        policy = format_policy(policy, project_id, user_name)
        responses.append(create_request(config=config, request=policy))
    print_formatted(config=config, output={'responses': responses})


@access_group.command(name="update")
@click.argument('request_id')
@click.argument('status')
@click.pass_obj
def access_update(config: Config, request_id: str, status: str):
    """Update the request's approval workflow.

    \b
    request_id (str): uuid of an existing request
    status (str): new status one of: DRAFT SUBMITTED APPROVED SIGNED REJECTED
    """
    with CLIOutput(config=config) as output:
        output.update(update(config, request_id, status))


@access_group.command(name="ls")
@click.option('--mine',  is_flag=True, show_default=True, default=False, help="List current user's requests. Otherwise, list all the requests the current user has access to see.")
@click.pass_obj
def access_ls(config: Config, mine: bool):
    """List current user's requests."""
    with CLIOutput(config=config) as output:
        output.update(ls(config, mine))


@access_group.command(name="cat")
@click.argument('request_id')
@click.pass_obj
def access_cat(config: Config, request_id: str):
    """Show details of a specific request.

    \b
    request_id (str): uuid of an existing request
    """
    with CLIOutput(config=config) as output:
        output.update(cat(config, request_id))
