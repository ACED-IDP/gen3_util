import datetime
import pathlib
import sys
import os
import click

from gen3_util import Config
from gen3_util.common import Push, write_meta_index, read_ndjson_file, Commit
from gen3_util.config import ensure_auth
from gen3_util.files.manifest import upload_files, upload_commit_to_indexd
from gen3_util.meta.publisher import publish_commits, re_publish_commits


def push(config: Config,
         restricted_project_id: str = None,
         overwrite_index: bool = False,
         overwrite_files: bool = False,
         upload_path: str = None,
         wait: bool = False,
         auth=None):
    """Push committed changes to commons."""
    assert config.gen3.project_id, "Not in an initialed project directory."
    push_ = Push(config=config)
    assert len(push_.pending_commits()) > 0, "No pending commits."

    if not auth:
        auth = ensure_auth(config=config)

    # first publish files to indexd and copy to bucket
    for commit in push_.pending_commits():
        click.echo(
            f"Pushing commit:{commit.commit_id} '{commit.message}' to {config.gen3.profile} {config.gen3.project_id}",
            file=sys.stderr
        )
        manifest_entries = upload_commit_to_indexd(
            config=config,
            commit=commit,
            overwrite_index=overwrite_index,
            restricted_project_id=restricted_project_id,
            auth=auth
        )
        # if len(manifest_entries) == 0:
        #     click.echo(f"INFO No files to upload for {commit.commit_id}", file=sys.stderr)

        click.echo(
            f"Indexed {len(manifest_entries)} files",
            file=sys.stderr
        )
        if not upload_path:
            upload_path = pathlib.Path.cwd()
        completed_process = upload_files(
            config=config,
            project_id=config.gen3.project_id,
            manifest_entries=manifest_entries,
            profile=config.gen3.profile,
            upload_path=upload_path,
            overwrite_files=overwrite_files,
            auth=auth
        )
        assert completed_process.returncode == 0, f"upload_files failed with {completed_process.returncode}"
        click.echo(
            f"Upload {len(manifest_entries)} files",
            file=sys.stderr
        )
        push_.commits.append(commit)

    published_job = publish_commits(
        config=config,
        push=push_,
        wait=wait,
        auth=auth
    )

    click.echo(
        f"Published {len(push_.commits)} commits",
        file=sys.stderr
    )

    completed_path = push_.config.commit_dir() / "completed.ndjson"
    push_.published_timestamp = datetime.datetime.now()
    push_.published_job = published_job
    with open(completed_path, "a") as fp:
        fp.write(push_.model_dump_json())
        fp.write("\n")
    click.echo(
        f"Updated {completed_path}",
        file=sys.stderr
    )

    pending_path = push_.config.commit_dir() / "pending.ndjson"
    pending_path.unlink(missing_ok=False)
    click.echo(
        f"Cleared {pending_path}",
        file=sys.stderr
    )

    if os.path.isfile(push_.config.commit_dir() / "emptied.ndjson"):
        pending_path = push_.config.commit_dir() / "emptied.ndjson"
        pending_path.unlink(missing_ok=False)
        click.echo(
            f"Cleared {pending_path}",
            file=sys.stderr
        )

    # index the cloned metadata
    write_meta_index(
        index_path=config.state_dir,
        source_path=(pathlib.Path.cwd() / 'META')
    )

    return published_job


def re_push(config: Config):
    """Re-publish last push's commits to the portal."""
    push_ = Push(config=config)
    completed_path = push_.config.commit_dir() / "completed.ndjson"
    last_push = None
    for _ in read_ndjson_file(completed_path):
        last_push = _
    assert last_push, f"No completed job found in {completed_path}"
    push_.commits = [Commit(**_) for _ in last_push['commits']]
    for _commit in push_.commits:
        click.secho(f"Re-publishing {_commit.commit_id} {_commit.message} ", fg='yellow')
    published_job = re_publish_commits(config, push=push_, wait=False)
    push_.published_timestamp = datetime.datetime.now()
    push_.published_job = published_job
    with open(completed_path, "a") as fp:
        fp.write(push_.model_dump_json())
        fp.write("\n")
    click.secho(
        f"Updated {completed_path}",
        fg='yellow',
        file=sys.stderr
    )
    return published_job
