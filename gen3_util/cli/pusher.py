import datetime
import pathlib
import sys

from gen3_util import Config
from gen3_util.common import Push
from gen3_util.config import ensure_auth
from gen3_util.files.manifest import upload_files, upload_commit_to_indexd
from gen3_util.meta.publisher import publish_commits


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
        auth = ensure_auth(profile=config.gen3.profile)

    # first publish files to indexd and copy to bucket
    for commit in push_.pending_commits():
        print(
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
        assert len(manifest_entries) > 0, "No manifest entries uploaded to ."
        print(
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
        print(
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

    print(
        f"Published {len(push_.commits)} commits",
        file=sys.stderr
    )

    completed_path = push_.config.commit_dir() / "completed.ndjson"
    push_.published_timestamp = datetime.datetime.now()
    push_.published_job = published_job
    with open(completed_path, "a") as fp:
        fp.write(push_.json())
        fp.write("\n")
    print(
        f"Updated {completed_path}",
        file=sys.stderr
    )

    pending_path = push_.config.commit_dir() / "pending.ndjson"
    pending_path.unlink(missing_ok=False)
    print(
        f"Cleared {pending_path}",
        file=sys.stderr
    )

    return published_job
