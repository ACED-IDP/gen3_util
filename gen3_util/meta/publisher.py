import asyncio
import json
import pathlib
import sys
import click

from gen3.jobs import Gen3Jobs

from gen3_util.common import Push
from gen3_util.meta.uploader import cp as cp_upload

from gen3_util.config import Config, ensure_auth


def publish_meta_data(config: Config, meta_data_path: str, ignore_state: bool, project_id: str, wait: bool = True) -> str:
    """Publish meta_data to the portal.

    Returns: the output of jobs_client.async_run_job_and_wait() should be a json string, with a key 'output'
    """
    msgs = []
    assert pathlib.Path(meta_data_path).is_dir(), f"{meta_data_path} is not a directory"
    assert project_id is not None, "--project_id is required for uploads"
    upload_result = cp_upload(config, meta_data_path, project_id, ignore_state)
    msgs.append(upload_result['msg'])  # TODO - why is this msgs here

    object_id = upload_result['object_id']
    auth = ensure_auth(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)
    args = {'object_id': object_id, 'project_id': project_id, 'method': 'put'}
    if wait:
        _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    else:
        _ = jobs_client.create_job('fhir_import_export', args)
        _ = {'output': json.dumps(_)}
    return _


def publish_commits(config: Config, push: Push, wait: bool = True, auth=None) -> dict:
    """Publish commits to the portal."""
    if not auth:
        auth = ensure_auth(config=config)
    user = auth.curl('/user/user').json()

    for commit in push.commits:
        upload_result = cp_upload(
            config=config,
            from_=commit.meta_path,
            project_id=config.gen3.project_id,
            ignore_state=True,
            auth=auth,
            user=user,
            metadata={'message': commit.message, 'is_commit': True}
        )
        commit.object_id = upload_result['object_id']
        click.echo(
            upload_result['msg'],
            file=sys.stdout
        )

    jobs_client = Gen3Jobs(auth_provider=auth)

    args = {'push': push.model_dump(), 'project_id': config.gen3.project_id, 'method': 'put'}
    if wait:
        _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
        _ = {'output': _}
    else:
        _ = jobs_client.create_job('fhir_import_export', args)
        _ = {'output': _}

    return _


def re_publish_commits(config: Config, push: Push, wait: bool = True, auth=None) -> dict:
    """Re-publish commits to the portal."""
    if not auth:
        auth = ensure_auth(config=config)

    jobs_client = Gen3Jobs(auth_provider=auth)

    args = {'push': push.model_dump(), 'project_id': config.gen3.project_id, 'method': 'put'}

    if wait:
        _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
        _ = {'output': _}
    else:
        _ = jobs_client.create_job('fhir_import_export', args)
        _ = {'output': _}

    return _
