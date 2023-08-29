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
@click.argument('project_id')
@click.argument('object_id')
@click.pass_obj
def import_meta(config: Config, project_id: str, object_id: str):
    """Import uploaded metadata .

    \b
    PROJECT_ID: <program-name>-<project-name>
    OBJECT_ID: indexd record id of uploaded metadata
    """
    auth = ensure_auth(config.gen3.refresh_file)
    jobs_client = Gen3Jobs(auth_provider=auth)
    args = {'object_id': object_id, 'project_id': project_id}

    _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import', args))
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
