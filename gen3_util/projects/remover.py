import asyncio
import sys
import os
import datetime
import click

from gen3.jobs import Gen3Jobs
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import ProjectSummaries
from gen3_util.repo import CLIOutput
from gen3_util.common import _check_parameters, Push
from gen3_util.repo.committer import delete_all_commits


def empty(config: Config, project_id: str, wait: bool = False) -> dict:
    """Empty all meta data (graph, flat) for a project."""

    assert '-' in project_id, f'Invalid project_id: {project_id}'
    program, project = project_id.split('-')
    assert program and project, f'Invalid project_id: {project_id}'

    auth = ensure_auth(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)
    args = {'object_id': None, 'project_id': project_id, 'method': 'delete'}

    if wait:
        _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    else:
        _ = jobs_client.create_job('fhir_import_export', args)
        _ = {'output': _}
    return _


def empty_all(config: Config):
    with CLIOutput(config=config) as output:
        try:
            assert config.gen3.project_id, "Not in an initialized project directory."
            project_id = config.gen3.project_id
            _check_parameters(config, project_id)

            _ = empty(config, project_id)
            _['msg'] = f"Emptied {project_id}"
            output.update(_)

            delete_all_commits(config.commit_dir())
            for file in [".g3t/state/manifest.sqlite", ".g3t/state/meta-index.ndjson"]:
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
                file=sys.stderr, fg='green'
            )

        except (AssertionError, Exception) as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


def reset_to_commit_id(config: Config, commit_id: str):
    with CLIOutput(config=config) as output:
        try:
            assert config.gen3.project_id, "Not in an initialized project directory."
            project_id = config.gen3.project_id
            _check_parameters(config, project_id)

        except (AssertionError, Exception) as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


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
