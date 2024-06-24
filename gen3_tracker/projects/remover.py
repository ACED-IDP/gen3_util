import asyncio

from gen3.jobs import Gen3Jobs

from gen3_tracker.config import Config, ensure_auth
from gen3_tracker.projects import ProjectSummaries


def empty(config: Config, project_id: str, wait: bool = True) -> dict:
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


def rm(config: Config, project_id: str) -> dict:
    """Remove a project."""

    # improve startup time by importing only what is needed
    from gen3.submission import Gen3Submission

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
