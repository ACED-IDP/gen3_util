import asyncio
import json
import subprocess
from json import JSONDecodeError

import click
from gen3.jobs import Gen3Jobs
from requests import HTTPError

from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
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
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.argument('object_id')
@click.pass_obj
def import_meta(config: Config, project_id: str, object_id: str):
    """Import metadata from bucket into portal.

    \b
    OBJECT_ID: indexd record id of uploaded metadata
    """
    auth = ensure_auth(profile=config.gen3.profile)
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


@job_group.command('export')
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.argument('path')
@click.pass_obj
def export_meta(config: Config, project_id: str, path: str):
    """Export project metadata to bucket file.

    \b
    PATH: path to save metadata
    """

    assert project_id, "--project_id required"
    assert project_id.count('-') == 1, "--project_id must be of the form program-project"
    auth = ensure_auth(profile=config.gen3.profile)

    # delivered to sower job in env['ACCESS_TOKEN']
    jobs_client = Gen3Jobs(auth_provider=auth)
    # delivered to sower job in env['INPUT_DATA']
    args = {'project_id': project_id, 'method': 'get'}

    _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    try:
        output = json.loads(_['output'])

        with CLIOutput(config=config) as console_output:
            object_id = output['object_id']

            cmd = f"gen3-client download-single --profile {config.gen3.profile} --guid {object_id} --download-path {path}".split()
            upload_results = subprocess.run(cmd)
            assert upload_results.returncode == 0, upload_results
            output['logs'].append(f"Downloaded {object_id} to {path}")

            if 'user' in output:
                del output['user']

            console_output.update(output)

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

    auth = ensure_auth(profile=config.gen3.profile)
    jobs_client = Gen3Jobs(auth_provider=auth)
    try:
        status = jobs_client.get_status(job_id)
    except HTTPError as e:
        status['msg'] = str(e)

    completed = True
    if 'status' not in status:
        status['msg'] = "Job not found"
        completed = False
    else:
        if status['status'].lower() != 'completed':
            status['msg'] = f"Job {job_id} is not complete, status: {status['status']}"
            completed = False

    if completed:
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
