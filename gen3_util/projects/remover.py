import asyncio
import sys
import os
from datetime import datetime
import click
import re
import json

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


def get_tuple_by_id(data, id_to_find):
    """return tuple by id"""
    for i, item in enumerate(data):
        if item[0] == id_to_find:
            return i, item
    return None, None


def reset_to_commit_id(config: Config, commit_id: str, project_id: str) -> dict:
    """Rollback locally and server side to an existing local commit state"""

    _check_parameters(config, project_id)
    with open(f".g3t/state/{project_id}/commits/completed.ndjson", "r") as f:
        commits = [json.loads(line) for line in f]

    commits_ids = [commit["commits"][0]["commit_id"] for commit in commits]
    dir_commits = [match.group() for string in os.listdir(f".g3t/state/{project_id}/commits")
                   if (match := re.search(r"([a-fA-F\d]{32})", string)) and len(match.group()) == 32]

    """Make sure that every commit exists in completed.ndjson"""
    for dir in dir_commits:
        assert dir in commits_ids, f"commit {dir} does not exist in {f'.g3t/state/{project_id}/commits/completed.ndjson'}"

    remove_list, updated_commits, manifest_ids = [], [], []
    commit_date_order = [(commit["commits"][0]["commit_id"], commit["published_timestamp"]) for commit in commits]
    sorted_dates = sorted(commit_date_order, key=lambda x: x[1])
    i, tuple_entry = get_tuple_by_id(sorted_dates, commit_id)

    """Make sure that all commits after commit_id are purged
    and all commits at or before the commit_id are kept"""
    if i is not None and tuple_entry is not None and \
       i < (len(sorted_dates) - 1):
        remove_list = [id[0] for id in sorted_dates[i+1:]]
        updated_commits = [commit for commit in commits[:i+1]]

    if len(remove_list) > 0 and len(updated_commits) > 0:
        for dir in remove_list:
            commit_path = f".g3t/state/{project_id}/commits/{dir}"
            if os.path.isdir(commit_path):
                delete_all_commits(commit_path)

        with open(f".g3t/state/{project_id}/commits/completed.ndjson", "w") as f:
            for entry in updated_commits:
                f.write(json.dumps(entry))
                f.write("\n")

        with open(f".g3t/state/{project_id}/commits/{commit_id}/meta-index.ndjson") as f, \
                open(".g3t/state/meta-index.ndjson", "w") as g:
            for line in f:
                manifest_ids.append(json.loads(line))
                g.write(line)

        args = {'object_id': None, 'project_id': project_id, 'method': 'delete', 'manifest_ids': manifest_ids}
        _ = empty(config, project_id, args)
        _['msg'] = f"Emptied {project_id}"
        return _
    else:
        raise AssertionError(f"Nothing to change, commit {commit_id} has no subsequent commits after it to purge")


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
