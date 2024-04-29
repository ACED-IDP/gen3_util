import asyncio
import sys
import os
from datetime import datetime
import click


from gen3.jobs import Gen3Jobs
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import ProjectSummaries
from gen3_util.repo import CLIOutput
from gen3_util.common import _check_parameters, Push
from gen3_util.repo.committer import delete_all_commits


def empty(config: Config, project_id: str, args: dict, wait: bool = False) -> dict:
    """Empty all meta data (graph, flat) for a project."""

    assert '-' in project_id, f'Invalid project_id: {project_id}'
    program, project = project_id.split('-')
    assert program and project, f'Invalid project_id: {project_id}'

    auth = ensure_auth(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)

    if wait:
        _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    else:
        _ = jobs_client.create_job('fhir_import_export', args)
        _ = {'output': _}
    return _


def empty_all(config: Config, output: CLIOutput, project_id: str) -> CLIOutput:
    _check_parameters(config, project_id)

    args = {'object_id': None, 'project_id': project_id, 'method': 'delete'}
    _ = empty(config, project_id, args)
    _['msg'] = f"Emptied {project_id}"
    output.update(_)

    delete_all_commits(config.commit_dir())
    for file in [f"{config.state_dir}/manifest.sqlite", f"{config.state_dir}/meta-index.ndjson"]:
        if os.path.isfile(file):
            os.unlink(file)

    push_ = Push(config=config)
    push_.published_job = _
    completed_path = push_.config.commit_dir() / "emptied.ndjson"
    push_.published_timestamp = datetime.now()

    with open(completed_path, "w") as fp:
        fp.write(push_.model_dump_json())
        fp.write("\n")
    click.secho(
        f"Updated {completed_path}",
        file=sys.stdout, fg='green'
    )


def get_tuple_by_id(data, id_to_find):
    """return tuple by id"""
    for i, item in enumerate(data):
        if item[0] == id_to_find:
            return i, item
    return None, None


def rm(config: Config, project_id: str) -> dict:
    """Remove a project."""

    assert '-' in project_id, f'Invalid project_id: {project_id}'
    program, project = project_id.split('-')
    assert program and project, f'Invalid project_id: {project_id}'

    auth = ensure_auth(config=config)
    submission = Gen3Submission(auth)

    response = submission.delete_project(program=program, project=project)
    response.raise_for_status()

    return ProjectSummaries(**{
        'endpoint': auth.endpoint,
        'projects': {project_id: {'exists': False}},
        'messages': [f'Deleted {project_id}']
    })
