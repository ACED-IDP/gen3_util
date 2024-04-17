import json
import pathlib
import shutil
import tempfile
import os
from collections import defaultdict
from hashlib import md5
from typing import Generator
from zipfile import ZipFile

from orjson import orjson
from pydantic import BaseModel

from gen3_util import Config
from gen3_util.common import read_ndjson_file, dict_md5, EmitterContextManager, Push, write_meta_index, read_meta_index
from gen3_util.files.manifest import ls as manifest_ls
from gen3_util.meta import ParseResult
from gen3_util.meta.validator import validate


class CommitResult(BaseModel):
    """Results of FHIR validation of directory."""
    resource_counts: dict = None
    exceptions: list[ParseResult] = None
    logs: list[str] = None
    message: str = None
    commit_id: str = None
    path: pathlib.Path = None
    manifest_files: list[str] = None

    def model_dump(self):
        """Dump the config model.

         temporary until we switch to pydantic2
        """
        for _ in self.exceptions:
            _.exception = str(_.exception)
            _.path = str(_.path)
        return orjson.loads(self.model_dump_json())


def diff(config, metadata_path) -> Generator[dict, None, None]:
    """Diff metadata with current state."""
    existing_meta_index = {_['id']: _['md5'] for _ in read_meta_index(config.state_dir)}
    pending_meta_index = {_['id']: _['md5'] for _ in Push(config=config).pending_meta_index()}

    for _ in sorted(metadata_path.glob("*.ndjson")):
        line = 0
        for resource in read_ndjson_file(_):
            line += 1
            # is it in the pending index?
            existing_md5 = pending_meta_index.get(resource['id'], None)
            if not existing_md5:
                # is it in the existing index?
                existing_md5 = existing_meta_index.get(resource['id'], None)
            resource_md5 = dict_md5(resource)
            if existing_md5 and (existing_md5 == resource_md5):
                continue
            # new resource or resource has changed
            new_changed = 'new' if not existing_md5 else 'changed'

            identifier = resource.get('identifier', None)
            if identifier:
                if not isinstance(identifier, list):
                    identifier = [identifier]
                _identifier = [_ for _ in identifier if _.get('use') == 'official']
                if len(_identifier) == 0:
                    _identifier = identifier[0]
                _identifier = _identifier[0]

            yield {
                'path': str(_),
                'line': line,
                'id': resource['id'],
                'identifier': _identifier.get('value'),
                'type': new_changed
            }


def prepare_metadata_zip(config, metadata_path) -> (str, pathlib.Path, list[str]):
    """Compress all  metadata zip for commit.
    returns the md5 string, the path to the zip file, and a list of logs,
    """
    project_id = config.gen3.project_id
    existing_meta_index = {_['id']: _['md5'] for _ in read_meta_index(config.state_dir)}
    pending_meta_index = {_['id']: _['md5'] for _ in Push(config=config).pending_meta_index()}
    resource_counts = defaultdict(int)

    # TODO - add metadata_index to clone
    with tempfile.TemporaryDirectory() as work_path:
        work_path = pathlib.Path(work_path)
        emitted_resource_md5s = []
        with EmitterContextManager(work_path, file_mode="w") as emitter:
            for _ in sorted(metadata_path.glob("*.ndjson")):
                for resource in read_ndjson_file(_):
                    # is it in the pending index?
                    existing_md5 = pending_meta_index.get(resource['id'], None)
                    if not existing_md5:
                        # is it in the existing index?
                        existing_md5 = existing_meta_index.get(resource['id'], None)
                    resource_md5 = dict_md5(resource)
                    if existing_md5 and (existing_md5 == resource_md5):
                        continue
                    # new resource or resource has changed
                    emitter.emit(resource['resourceType']).write(
                        orjson.dumps(resource, option=orjson.OPT_APPEND_NEWLINE).decode('utf-8')
                    )
                    emitted_resource_md5s.append(resource_md5)
                    resource_counts[resource['resourceType']] += 1

        # there should be at least one resource changed
        assert len(emitted_resource_md5s) > 0, f"Metadata resources in {metadata_path} remain unchanged\
but additional files have been staged. Add corresponding metadata files or generate skeleton metdata with 'g3t util meta create'"
        # aggregate md5 for all resources
        m = md5()
        for _ in emitted_resource_md5s:
            m.update(_.encode())
        md5_string = m.hexdigest()

        object_name = 'meta.zip'

        commit_path = config.state_dir / project_id / 'commits' / md5_string
        assert not commit_path.exists(), f"{md5_string} already exists,  a commit with the same metadata already exists"
        commit_path.mkdir(parents=True, exist_ok=True)
        zipfile_path = commit_path / object_name
        with ZipFile(zipfile_path, 'w') as zip_object:
            for _ in sorted(work_path.glob("*.ndjson")):
                zip_object.write(_, arcname=_.name)

        # write an index of ids and hashes used in this commit
        write_meta_index(
            index_path=commit_path,
            source_path=work_path
        )

        return md5_string, zipfile_path, [], resource_counts


def save_commit_object(config, commit_id, commit_object: dict, commit_path: pathlib.Path):
    """Serialize the commit object to a file."""
    with open(commit_path / "resource.json", "w") as f:
        f.write(orjson.dumps(commit_object, option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE).decode())


def delete_all_commits(path):
    """Deletes a directory and all its contents, recursively using os.unlink and os.rmdir. """
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        if os.path.isfile(item_path):
            os.unlink(item_path)
        else:
            delete_all_commits(item_path)

    if not str(path).endswith("commits"):
        os.rmdir(path)


def commit(config: Config, metadata_path: pathlib.Path, files_path: pathlib.Path, commit_message: str) -> CommitResult:
    """Validate new metadata and files."""

    exceptions = []
    resource_counts = defaultdict(int)
    logs = []

    result = validate(config=config, directory_path=metadata_path)

    # TODO - validate DocumentReference attachments after they pass diff
    # if parse_result.resource.resource_type == 'DocumentReference':
    #     document_reference: DocumentReference = parse_result.resource
    #     attachment_path = files_path / document_reference.content[0].attachment.url.replace('file:///', '')
    #     if not attachment_path.exists():
    #         relative_path = attachment_path.relative_to(pathlib.Path.cwd())
    #         parse_result.exception = f"FileNotFoundError {relative_path}"
    #         parse_result.resource = None
    #         exceptions.append(parse_result)
    #         continue
    #     # check if document is in the current manifest
    #     if document_reference.id not in manifest:
    #         parse_result.exception = f"DocumentReference {document_reference.id} not in manifest"
    #         parse_result.resource = None
    #         exceptions.append(parse_result)
    #         continue

    if len(result.exceptions) > 0:
        logs.append(f"Validation failed, {len(result.exceptions)} exceptions")
        for _ in result.exceptions:
            logs.append(f"{_.exception}")
        commit_result = CommitResult(
            resource_counts=dict(resource_counts),
            exceptions=exceptions,
            logs=logs,
            message="Failed validation",
        )
        return commit_result
    else:
        logs.append("Validation passed")
        commit_id, zipfile_path, prepare_logs, resource_counts = prepare_metadata_zip(config, metadata_path)
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
        # now save the commit object
        save_commit_object(config, commit_id, commit_result.model_dump(), path)
        # move the manifest to the commit directory
        manifest_path = config.state_dir / 'manifest.sqlite'
        if manifest_path.exists():
            shutil.move(manifest_path, path / manifest_path.name)
        # add the commit to the pending list
        with open(path.parent / "pending.ndjson", "at") as f:
            f.write(orjson.dumps({'commit_id': commit_id},
                                 option=orjson.OPT_SORT_KEYS | orjson.OPT_APPEND_NEWLINE).decode())

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
