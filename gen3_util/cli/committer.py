import json
import pathlib
import shutil
from collections import defaultdict
from zipfile import ZipFile

from fhir.resources.documentreference import DocumentReference
from orjson import orjson
from pydantic import BaseModel

from gen3_util import Config
from gen3_util.common import calc_md5, read_ndjson_file
from gen3_util.meta import directory_reader, ParseResult
from gen3_util.files.manifest import ls as manifest_ls
from hashlib import md5


class CommitResult(BaseModel):
    """Results of FHIR validation of directory."""
    resource_counts: dict = None
    exceptions: list[ParseResult] = None
    logs: list[str] = None
    message: str = None
    commit_id: str = None
    path: pathlib.Path = None

    def model_dump(self):
        """Dump the config model.

         temporary until we switch to pydantic2
        """

        return orjson.loads(self.json())


def prepare_metadata_zip(config, metadata_path) -> (str, pathlib.Path, list[str]):
    """Compress all  metadata zip for commit.
    returns the md5 string, the path to the zip file, and a list of logs,
    """
    project_id = config.gen3.project_id

    m = md5()

    # TODO unfortunately we need to read all the meta of all contents.
    #  how could we do this faster, without reading all the files?
    for _ in metadata_path.glob("*.ndjson"):
        m = calc_md5(_, m)
    md5_string = m.hexdigest()

    object_name = 'meta.zip'

    commit_path = config.state_dir / project_id / 'commits' / md5_string
    assert not commit_path.exists(), f"{commit_path} already exists,  a commit with the same metadata already exists"
    commit_path.mkdir(parents=True, exist_ok=True)
    zipfile_path = commit_path / object_name
    with ZipFile(zipfile_path, 'w') as zip_object:
        for _ in sorted(metadata_path.glob("*.ndjson")):
            zip_object.write(_, arcname=_.name)

    # write a index of ids
    index_path = commit_path / 'meta-index.ndjson'
    with open(index_path, 'w') as fp:
        for _ in sorted(metadata_path.glob("*.ndjson")):
            for resource in read_ndjson_file(_):
                _ = {'id': resource['id'], 'resourceType': resource['resourceType']}
                fp.write(orjson.dumps(_).decode())
                fp.write('\n')

    return md5_string, zipfile_path, []


def save_commit_object(config, commit_id, commit_object: dict, commit_path: pathlib.Path):
    """Serialize the commit object to a file."""
    with open(commit_path / "resource.json", "w") as f:
        f.write(orjson.dumps(commit_object, option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE).decode())
    manifest_path = config.state_dir / 'manifest.sqlite'
    shutil.move(manifest_path, commit_path / manifest_path.name)
    with open(commit_path.parent / "pending.ndjson", "at") as f:
        f.write(orjson.dumps({'commit_id': commit_id}, option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE).decode())


def commit(config: Config, metadata_path: pathlib.Path, files_path: pathlib.Path, commit_message: str) -> CommitResult:
    """Validate new metadata and files."""

    exceptions = []
    resource_counts = defaultdict(int)
    logs = []

    manifest = {_['object_id']: _ for _ in manifest_ls(config, project_id=config.gen3.project_id)}

    for parse_result in directory_reader(metadata_path):

        if parse_result.exception:
            exceptions.append(parse_result)
        else:
            if parse_result.resource.resource_type == 'DocumentReference':
                document_reference: DocumentReference = parse_result.resource
                attachment_path = files_path / document_reference.content[0].attachment.url.replace('file:///', '')
                if not attachment_path.exists():
                    relative_path = attachment_path.relative_to(pathlib.Path.cwd())
                    parse_result.exception = f"FileNotFoundError {relative_path}"
                    parse_result.resource = None
                    exceptions.append(parse_result)
                    continue
                # check if document is in the current manifest
                if document_reference.id not in manifest:
                    parse_result.exception = f"DocumentReference {document_reference.id} not in manifest"
                    parse_result.resource = None
                    exceptions.append(parse_result)
                    continue
            resource_counts[parse_result.resource.resource_type] += 1
    if len(exceptions) > 0:
        logs.append(f"Validation failed, {len(exceptions)} exceptions")
        commit_result = CommitResult(
            resource_counts=dict(resource_counts),
            exceptions=exceptions,
            logs=logs,
            message="Failed validation",
        )
        return commit_result
    else:
        logs.append("Validation passed")
        commit_id, zipfile_path, prepare_logs = prepare_metadata_zip(config, metadata_path)
        logs.extend(prepare_logs)
        path = zipfile_path.parent
        commit_result = CommitResult(
            resource_counts=dict(resource_counts),
            exceptions=exceptions,
            logs=logs,
            commit_id=commit_id,
            path=path,
            message=commit_message
        )
        save_commit_object(config, commit_id, commit_result.model_dump(), path)
        logs.append(f"{metadata_path} ready to push")
        return commit_result


def commit_status(config, project_id):
    """Get the local list of commits and the remote counts."""
    commits_path = config.state_dir / project_id / 'commits'
    pending_path = commits_path / 'pending.ndjson'
    pending_commits = []
    if pending_path.exists():
        for _ in read_ndjson_file(pending_path):
            manifest = [_['file_name'] for _ in manifest_ls(config, project_id=config.gen3.project_id, commit_id=_.get('commit_id'))]
            commit_ = json.load(open(commits_path / _.get('commit_id') / 'resource.json'))
            commit_['manifest'] = manifest
            pending_commits.append(commit_)

    return pending_commits
