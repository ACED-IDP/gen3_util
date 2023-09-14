import asyncio
import json
from json import JSONDecodeError

import click
from gen3.jobs import Gen3Jobs

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
              help="Gen3 program-project")
@click.argument('object_id')
@click.pass_obj
def import_meta(config: Config, project_id: str, object_id: str):
    """Import uploaded metadata .

    \b
    PROJECT_ID: <program-name>-<project-name>
    OBJECT_ID: indexd record id of uploaded metadata
    """
    auth = ensure_auth(config.gen3.refresh_file)
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
              help="Gen3 program-project")
@click.argument('path')
@click.pass_obj
def export_meta(config: Config, project_id: str, path: str):
    """Export uploaded metadata .

    \b
    PROJECT_ID: <program-name>-<project-name>
    OBJECT_ID: indexd record id of uploaded metadata
    """
    assert project_id, "--project_id required"
    assert project_id.count('-') == 1, "--project_id must be of the form program-project"
    auth = ensure_auth(config.gen3.refresh_file)
    # delivered to sower job in env['ACCESS_TOKEN']
    jobs_client = Gen3Jobs(auth_provider=auth)
    # delivered to sower job in env['INPUT_DATA']
    args = {'project_id': project_id, 'method': 'get'}

    _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    try:
        output = json.loads(_['output'])

        with CLIOutput(config=config) as console_output:
            console_output.update(output)

    except JSONDecodeError:
        print("jobs_client.async_run_job_and_wait() (raw):", _)


@job_group.command('get')
@click.argument('job_id')
@click.pass_obj
def get(config: Config, job_id):
    """Get a job.
    \b
    JOB_ID: uuid of job
    """
    auth = ensure_auth(config.gen3.refresh_file)
    jobs_client = Gen3Jobs(auth_provider=auth)

    _ = jobs_client.get_output(job_id)
    # print("jobs_client.get_output() (raw):", _)
    error = False
    if 'output' not in _:
        error = True
    try:
        output = json.loads(_['output'])
        with CLIOutput(config=config) as output:
            output.update(output)
    except JSONDecodeError:
        error = True
    if error:
        print("jobs_client.get_output() (raw):", _)
