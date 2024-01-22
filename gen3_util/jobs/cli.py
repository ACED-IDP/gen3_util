import asyncio
import json
from json import JSONDecodeError

import click
from gen3.jobs import Gen3Jobs
from requests import HTTPError

from gen3_util.repo import CLIOutput, ENV_VARIABLE_PREFIX
from gen3_util.repo import NaturalOrderGroup
from gen3_util.config import Config, ensure_auth
from gen3_util.jobs.lister import ls


@click.group(name='jobs', cls=NaturalOrderGroup)
@click.pass_obj
def job_group(config: Config):
    """Manage Gen3 jobs."""
    pass


@job_group.command(name="ls")
@click.pass_obj
def project_ls(config: Config):
    """List all jobs user has access to."""
    with CLIOutput(config=config) as output:
        output.update(ls(config))


@job_group.command('import')
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.argument('object_id')
@click.pass_obj
def import_meta(config: Config, project_id: str, object_id: str):
    """Import metadata from bucket into portal.

    \b
    OBJECT_ID: indexd record id of uploaded metadata
    """
    auth = ensure_auth(config=config)
    # delivered to sower job in env['ACCESS_TOKEN']
    jobs_client = Gen3Jobs(auth_provider=auth)
    # delivered to sower job in env['INPUT_DATA']
    args = {'object_id': object_id, 'project_id': project_id, 'method': 'put'}

    _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    try:
        _ = json.loads(_['output'])
        with CLIOutput(config=config) as output:
            output.update(_)

    except JSONDecodeError:
        print("jobs_client.async_run_job_and_wait() (raw):", _)


@job_group.command('get')
@click.argument('job_id')
@click.pass_obj
def get(config: Config, job_id):
    """Display a job output
    \b
    JOB_ID: uuid of job
    """
    status = {'output': None}

    auth = ensure_auth(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)
    try:
        status = jobs_client.get_status(job_id)
    except HTTPError as e:
        status['msg'] = str(e)

    if 'status' not in status:
        status['msg'] = "Job not found"
    else:
        if status['status'].lower() != 'completed':
            status['msg'] = f"Job {job_id} is not complete, status: {status['status']}"

    _ = jobs_client.get_output(job_id)

    if 'output' not in _:
        status['msg'] = f"Job output not found  (raw): {_}"
    else:
        try:
            status['output'] = json.loads(_['output'])
        except JSONDecodeError:
            status['output'] = _['output']

    with CLIOutput(config=config) as output:
        output.update(status)
