import json
import logging
import mimetypes
import multiprocessing
import os
import pathlib
import subprocess
import time
import typing
import zipfile
from abc import abstractmethod
from datetime import datetime
from typing import NamedTuple

import inflection
import pydantic
import pytz
import yaml
from fhir.resources.attachment import Attachment
from gen3.auth import Gen3Auth
from pydantic import BaseModel, ConfigDict, field_validator

from gen3_tracker.common import ACCEPTABLE_HASHES, parse_iso_tz_date, create_object_id

# constants ---------------------------------------------------------------------
INIT_MESSAGE = 'Initializing a new repository...'
ADD_MESSAGE = 'Adding files to the repository...'
COMMIT_MESSAGE = 'Committing changes to the repository...'
PUSH_MESSAGE = 'Pushing changes to the remote repository...'
RESET_MESSAGE = 'Resetting changes in the repository...'
UNDO_INIT_MESSAGE = 'Removing change control from repository...'
MISSING_GIT_MESSAGE = '.git is not initialized in the current directory.'
MISSING_G3T_MESSAGE = '.g3t is not initialized in the current directory.'
STATUS_MESSAGE = 'Showing changed stages...'
DIFF_MESSAGE = 'Showing details of changed files...'

LOGGED_ALREADY = set()

mimetypes.add_type('text/fastq', '.fastq')
mimetypes.add_type('text/fastq', '.fq')

# process helpers ---------------------------------------------------------------


class CommandResult(NamedTuple):
    """
    A NamedTuple that represents the result of a command execution.

    Attributes:
        stdout (str): The standard output of the command.
        stderr (str): The standard error of the command.
        return_code (int): The return code of the command.
    """
    stdout: str
    stderr: str
    return_code: int


# https://dvc.org/doc/user-guide/project-structure/dvc-files#dvc-files
class DVCItem(BaseModel):
    hash: str

    md5: typing.Optional[str] = None
    sha256: typing.Optional[str] = None
    sha1: typing.Optional[str] = None
    sha512: typing.Optional[str] = None
    crc: typing.Optional[str] = None
    etag: typing.Optional[str] = None

    mime: typing.Optional[str] = 'application/octet-stream'
    modified: str
    path: str
    size: int
    is_symlink: typing.Optional[bool] = False
    realpath: typing.Optional[str] = None
    """For symlinked files, the real path to the file."""
    source_url: typing.Optional[str] = None
    """bucket imports, the url to the file."""

    object_id: typing.Optional[str] = None

    @pydantic.model_validator(mode="after")
    def check_hash_value(self):
        """Check that the hash value is valid."""
        hash_type = self.hash
        assert hash_type in ACCEPTABLE_HASHES, f'Invalid hash type {hash_type}'
        v = getattr(self, hash_type)
        assert ACCEPTABLE_HASHES[hash_type](v), f'Invalid {hash_type} {v}'
        self.modified = parse_iso_tz_date(self.modified)
        return self

    @property
    def hash_value(self):
        """Get the hash value."""
        return getattr(self, self.hash)

    def set_object_id(self, project_id: str) -> str:
        """ create a unique did for this object within a project"""
        assert self.path, 'path is required'

        self.object_id = create_object_id(self.path, project_id)
        return self.object_id


class DVCMeta(BaseModel):
    model_config = ConfigDict(
        extra='allow',
    )

    patient: typing.Optional[str] = None
    specimen: typing.Optional[str] = None
    observation: typing.Optional[str] = None
    task: typing.Optional[str] = None
    no_bucket: typing.Optional[bool] = False


class DVC(BaseModel):
    model_config = ConfigDict(
        extra='allow',
    )
    meta: typing.Optional[DVCMeta] = None
    outs: list[DVCItem]
    project_id: typing.Optional[str] = None

    @field_validator('outs')
    def check_outs(cls, v):
        if len(v) < 1:
            raise ValueError('outs must contain at least one item')
        return v

    @pydantic.model_validator(mode="after")
    def check_object_id(self):
        """Set the object_id if empty."""
        if not self.object_id and self.project_id:
            self.outs[0].set_object_id(self.project_id)
        return self

    @property
    def out(self) -> DVCItem:
        """Convenience method to get the first out."""
        return self.outs[0]

    @property
    def object_id(self) -> str:
        """Convenience method to get the first object_id."""
        if not self.outs[0].object_id and self.project_id:
            self.outs[0].set_object_id(self.project_id)
        return self.outs[0].object_id

    @classmethod
    def from_document_reference(cls, config, document_reference, references) -> 'DVC':
        """Factory: create DVC from DocumentReference.

        Args:
            config (object): our config object
            document_reference (DocumentReference): loaded DocumentReferences
            references (dict): cross ref of Reference to Identifiers

        """
        attachment: Attachment = document_reference.content[0].attachment
        assert attachment.extension, document_reference
        source_path = next(iter([_.valueUrl for _ in attachment.extension if _.url.endswith('source_path')]), None)
        if not source_path:
            source_path = attachment.url

        for k in ACCEPTABLE_HASHES.keys():
            if [_.valueString for _ in attachment.extension if _.url.endswith(k)]:
                hash_value = [_.valueString for _ in attachment.extension if _.url.endswith(k)][0]
                hash_name = k
                break

        assert source_path, ("missing source_path", document_reference)
        assert hash_value, ("missing hash_value", document_reference)

        resource_type, resource_id = document_reference.subject.reference.split('/')
        meta = DVCMeta()

        if not resource_type == 'ResearchStudy':
            resource_type = inflection.underscore(resource_type)
            identifier = references[document_reference.subject.reference]
            meta = DVCMeta(**{resource_type: identifier})

        modified = None
        if attachment.creation:
            modified = attachment.creation.isoformat()
        elif document_reference.date:
            modified = document_reference.date.isoformat()

        assert modified, ("missing attachment.creation, document_reference.date", document_reference)

        dvc_object = DVC(
            project_id=config.gen3.project_id,
            meta=meta,
            outs=[
                DVCItem(
                    modified=modified,
                    path=attachment.url.replace('file:///', ''),
                    size=attachment.size,
                    realpath=source_path.replace('file:///', ''),
                    mime=attachment.contentType,
                    object_id=document_reference.id,
                    **{hash_name: hash_value, 'hash': hash_name}
                )
            ],
        )

        if document_reference.subject:

            if document_reference.subject.reference in references:
                key = document_reference.subject.reference.split('/')[0]

                if key not in ['ResearchStudy']:
                    value = references[document_reference.subject.reference]
                    meta = DVCMeta.model_validate({key: value})
                    assert isinstance(meta, DVCMeta), f"Did not get expected meta {meta}"
                    dvc_object.meta = meta
            else:
                msg = f"Did not find {document_reference.subject.reference} in references"
                if msg not in LOGGED_ALREADY:
                    logging.getLogger(__package__).warning(msg)
                    LOGGED_ALREADY.add(msg)
        assert attachment.url.replace('file:///', '') == dvc_object.out.path, "attachment and dvc path doesn't match"
        assert document_reference.id == dvc_object.object_id, f"Did not get expected ID {config.gen3.project_id} {attachment.url}/{document_reference.id} {dvc_object.out.path}/{dvc_object.object_id}"
        return dvc_object


def run_command(command: str, dry_run: bool = False, raise_on_err: bool = True, env: dict = None, no_capture: bool = False) -> CommandResult:
    """Run a shell command and return its output. Raise an exception if the command fails."""
    _logger = logging.getLogger(__package__)
    if dry_run:
        _logger.info(f'dry_run: {command}')
        return CommandResult('', '', 0)
    else:
        if no_capture:
            process = subprocess.run(command, shell=True, env=env, check=True)
            stdout = b''
            stderr = b''
        else:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, env=env)
            stdout, stderr = process.communicate()

        if raise_on_err and process.returncode != 0:
            raise Exception(f'Command `{command}` failed with error code {process.returncode}, stderr: {stderr.decode()}')
        rc = CommandResult(stdout.decode(), stderr.decode(), process.returncode)
        _logger.debug(f'Command `{command}` returned {rc}')
        return rc


# git helpers -------------------------------------------------------------------
def git_remote_exists(dry_run: bool = False) -> bool:
    """Check if git repo has a remote."""

    stdout, stderr, return_code = run_command('git remote -v', dry_run=dry_run, raise_on_err=False)
    if '' != stdout:
        print('git_remote_exists', f">{stdout}<")
        return True
    return False


def git_repository_exists(dry_run: bool = False) -> bool:
    """Check if git is already initialized"""

    stdout, stderr, return_code = run_command('git status', dry_run=dry_run, raise_on_err=False)
    if return_code != 0:
        return False
    return True


def git_status() -> dict:
    """Get the status of the git repository"""
    result = run_command('git status -s --branch')
    assert result.return_code == 0, result.stderr
    path_statuses = []
    for line in result.stdout.split('\n'):
        if not line:
            continue
        if line.startswith('##'):
            branch = line.split(' ')[1]
            continue
        if not line.startswith('??'):
            _status, path = line.split()
            path_statuses.append({'status': _status, 'path': path})
            continue
    return branch, path_statuses


def git_ls(dry_run: bool = False) -> list[dict]:
    """List the files in the git repository"""
    results = run_command('git ls-files', dry_run=dry_run)
    return json.loads(results.stdout)


def git_files(dry_run=False) -> list[str]:
    """List the files in the git repository"""
    # """
    # %h = abbreviated commit hash
    # %x09 = tab (character for code 9)
    # %an = author name
    # %ad = author date (format respects --date= option)
    # %s = subject
    # """
    # get all files in the repository
    result = run_command('git log --pretty=format:"%h%x09%an%x09%ad%x09%s"  --date=iso-strict ', dry_run=dry_run, raise_on_err=False)
    if result.return_code == 0:
        git_logs = [_.split('\t') for _ in result.stdout.split('\n')]
        git_logs = [{'hash': _[0], 'author': _[1], 'timestamp': _[2], 'subject': _[3]} for _ in git_logs]
        for _ in git_logs:
            # diff = run_command(f'git diff --name-only {_["hash"]}~1 {_["hash"]}', ctx.obj['DRY_RUN'])
            files = run_command(f'git ls-tree --name-only -r {_["hash"]}', dry_run=dry_run)
            _['files'] = [_ for _ in files.stdout.split('\n') if _ != '']
        # de-duplicate the files
        to_upload = set()
        for _ in git_logs:
            to_upload.update([_ for _ in _['files'] if _.startswith('MANIFEST')])
            break
        return list(to_upload)
    return []


# file helpers ------------------------------------------------------------------
def get_mime_type(file_name):
    """Get mime type from file name."""
    return mimetypes.guess_type(file_name, strict=False)[0] or 'application/octet-stream'


def calculate_hash(hash_type, file_name):
    """Calculate the MD5 hash of a file."""
    assert hash_type == 'md5', f'Hash type {hash_type} is not supported (yet'
    import hashlib
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def git_archive(zip_name):
    """Archive the current branch and it's content to a zip file."""
    result = run_command("git rev-parse --abbrev-ref HEAD", no_capture=False)
    assert result.return_code == 0, f"Could not get current branch {result.stderr}"
    branch = result.stdout.strip()
    assert branch, "Could not get current branch"
    # add all the content
    run_command(f"git archive -o {zip_name} {branch}", no_capture=False)
    # add the .git folder
    with zipfile.ZipFile(zip_name, 'a') as zipf:  # 'a' for append mode
        for root, dirs, files in os.walk('.git'):
            for file in files:
                zipf.write(os.path.join(root, file),
                           os.path.relpath(os.path.join(root, file),
                           os.path.join('.git', '..')))


def modified_date(file_path):
    return datetime.fromtimestamp(os.path.getmtime(file_path), pytz.UTC).isoformat()


# manifest helpers --------------------------------------------------------------

class ManifestChange(NamedTuple):
    """A NamedTuple that represents a change in the manifest file."""
    data_path: pathlib.Path
    dvc_path: pathlib.Path


def data_file_changes(manifest_path, update: bool = False) -> list[ManifestChange]:
    """Check for changes in the dvc files timestamps, return the data path and the dvc file"""
    changes = []
    for _ in manifest_path.rglob('*.dvc'):
        dvc = to_dvc(_)

        if not dvc.out.realpath or dvc.out.source_url:
            continue

        data_path = pathlib.Path(dvc.out.realpath)

        if not data_path.is_symlink() and not data_path.exists():
            continue

        if data_path.stat().st_mtime > _.stat().st_mtime:
            if update:
                with open(_, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    out = yaml_data['outs'][0]
                    out['size'] = data_path.stat().st_size
                    out['modified'] = datetime.fromtimestamp(data_path.stat().st_mtime, pytz.UTC).isoformat()
                    out[out['hash']] = calculate_hash(out['hash'], data_path)
                    yaml_data['outs'] = [out]
                with open(_, 'w') as f:
                    yaml.dump(yaml_data, f, default_flow_style=False)
            #
            # return the data path and the dvc file
            #
            changes.append(ManifestChange(data_path, _))
    return changes


# meta data helpers ------------------------------------------------------------

def update_meta(file_path, new_meta):
    # Check if file exists
    # lazy loading improves startup
    from deepdiff import DeepDiff

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")

    # Get file attributes
    file_stat = os.stat(file_path)
    file_attributes = {
        'size': file_stat.st_size,
        'modified': file_stat.st_mtime,
    }

    # Load existing data from the YAML file
    with open(file_path, 'r') as f:
        yaml_data = yaml.safe_load(f)

    # Check if file attributes in the YAML data match the actual file attributes
    if yaml_data['outs'][0]['size'] == file_attributes['size'] and \
       yaml_data['outs'][0]['modified'] == file_attributes['modified']:
        # If file attributes haven't changed, check if the metadata has changed
        if 'meta' in yaml_data and DeepDiff(yaml_data['meta'], new_meta, ignore_order=True):
            # If metadata has changed, update it
            yaml_data['meta'] = new_meta

            # Write the updated data back to the YAML file
            with open(file_path, 'w') as f:
                yaml.dump(yaml_data, f, default_flow_style=False)
            return True

    return False


# gen3 helpers ------------------------------------------------------------------

def dvc_data(committed_files) -> typing.Generator[DVC, None, None]:
    """Get the dvc data from the committed files."""
    for committed_file in committed_files:
        if str(committed_file).endswith('.dvc'):
            yield to_dvc(committed_file)


def to_dvc(path) -> DVC:
    """Get the dvc data from a file."""
    with open(path, 'r') as f:
        _ = yaml.safe_load(f)
        return DVC.model_validate(_)


class LoggingWriter:
    def __init__(self, log_file=None):
        assert log_file is not None, 'log_file is required'
        pathlib.Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(log_file).unlink(missing_ok=True)
        self.log_file = log_file
        self.logger = None

    def __enter__(self):
        self.logger = logging.getLogger('LoggingWriter')
        handler = logging.FileHandler(self.log_file)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        # Prevent logging to console
        self.logger.propagate = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        handlers = self.logger.handlers[:]
        for handler in handlers:
            handler.close()
            self.logger.removeHandler(handler)

    @abstractmethod
    def save(self, _dvc: dict) -> typing.Any:
        pass


class MockIndexdWriter(LoggingWriter):
    def save(self, dvc: DVC) -> str:
        self.logger.info(f'Saving {dvc}')
        return 'OK'


class IndexdWriter(LoggingWriter):
    """Submit a job to the indexd service, return response."""

    def __init__(self, log_file, auth: Gen3Auth, project_id: str, bucket_name: str, overwrite: bool, restricted_project_id: str, existing_ids: list[str]):
        super().__init__(log_file)
        self.auth = auth
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.overwrite = overwrite
        self.restricted_project_id = restricted_project_id
        self.existing_ids = existing_ids

    def save(self, dvc: DVC) -> str:
        from gen3_tracker.gen3.indexd import write_indexd
        self.logger.debug(f'Saving {dvc}')
        write_indexd(
            auth=self.auth,
            dvc=dvc,
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            overwrite=self.overwrite,
            restricted_project_id=self.restricted_project_id,
            existing_records=self.existing_ids
        )
        return 'OK'


class MockJobWriter(LoggingWriter):
    """Submit a job to the submission service, return response."""

    def save(self, _dvc: dict) -> dict:
        self.logger.info(f'Saving {_dvc}')
        return {'job_info': ["TODO"]}  # {"job_info": ["TODO


class MockRemoteWriter(LoggingWriter):

    def __init__(self, log_file=None, remote=None):
        assert remote is not None, 'remote is required'
        super().__init__(log_file)
        self.remote = remote

    def save(self, _dvc: dict) -> str:
        self.logger.info(f'Saving to {self.remote} {_dvc}')
        return 'OK'


def to_manifest(dvc):
    return {
        'object_id': dvc.object_id,
        'md5': dvc.out.md5,
        'file_name': dvc.out.path,
        'size': dvc.out.size
    }


def to_s3(dvc):

    src = dvc.out.path
    if not pathlib.Path(src).exists():
        src = dvc.out.realpath
        if not src or not pathlib.Path(src).exists():
            src = dvc.out.source_url

    assert src, f"Could not determine source file. {dvc} "
    return f"aws s3 cp {src} s3://BUCKET-NAME/{dvc.object_id}/{dvc.out.path}"


class S3RemoteWriter(LoggingWriter):
    """Write files to S3."""

    def __init__(self, log_file=None, remote=None, work_dir=None):
        assert remote is not None, 'remote is required'
        super().__init__(log_file)
        self.remote = remote
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")  # Format datetime as you need
        manifest_file = pathlib.Path(work_dir) / f'sh-manifest-{current_time}.sh'
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_file_path = manifest_file
        self.manifest = []

    def save(self, dvc: DVC) -> str:
        self.logger.info(f'Saving to S3 {dvc}')
        self.manifest.append(to_s3(dvc))
        return 'OK'

    def commit(self, dry_run=False, profile=None, upload_path=None, bucket_name=None, worker_count=(multiprocessing.cpu_count() - 1)):
        with open(self.manifest_file_path, 'w') as f:
            for _ in self.manifest:
                _ = _.replace('BUCKET-NAME', bucket_name)
                f.write(f"{_}\n")
        print(f'See manifest file: {self.manifest_file_path}')
        return 'OK'


class Gen3ClientRemoteWriter(LoggingWriter):
    """Write files to Gen3 using gen3-client."""

    def __init__(self, log_file=None, remote=None, work_dir=None):
        assert remote is not None, 'remote is required'
        super().__init__(log_file)
        self.remote = remote
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")  # Format datetime as you need
        manifest_file = pathlib.Path(work_dir) / f'manifest-{current_time}.json'
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        self.manifest_file_path = manifest_file
        self.manifest = []

    def save(self, dvc: DVC) -> str:
        if dvc.out.realpath and not dvc.meta.no_bucket:
            self.logger.info(f'Saving to {self.remote} {dvc}')
            self.manifest.append(to_manifest(dvc))
        return 'OK'

    @staticmethod
    def default_worker_count():
        """Get the default worker count."""
        if 'G3T_NUM_PARALLEL' in os.environ:
            return int(os.environ.get('G3T_NUM_PARALLEL'))
        return multiprocessing.cpu_count() - 1

    def commit(self, dry_run=False, profile=None, upload_path=None, bucket_name=None, worker_count=default_worker_count()):
        with open(self.manifest_file_path, 'w') as f:
            json.dump(self.manifest, f)
        if len(self.manifest) > 0:
            cmd = f"gen3-client upload-multiple --manifest {self.manifest_file_path} --profile {profile} --upload-path {upload_path} --bucket {bucket_name} --numparallel {worker_count}"
            print(cmd)
            run_command(cmd, dry_run=dry_run, raise_on_err=True, no_capture=True)
        else:
            print(f'No files to upload to {self.remote} by gen3-client.')
        return 'OK'


def to_indexd(dvc_objects: list[DVC],
              auth: Gen3Auth,
              project_id: str,
              bucket_name: str,
              overwrite: bool,
              restricted_project_id: str
              ) -> typing.Generator[typing.Any, None, None]:
    """Upload committed files to indexd."""
    # indexd_writer = MockIndexdWriter
    # log_file = "logs/mock-indexd.log"
    indexd_writer = IndexdWriter
    log_file = "logs/indexd.log"

    existing_ids = []
    for _ in dvc_objects:
        _.project_id = project_id
        existing_ids.append(_.object_id)

    with indexd_writer(auth=auth,
                       log_file=log_file,
                       project_id=project_id,
                       bucket_name=bucket_name,
                       overwrite=overwrite,
                       existing_ids=existing_ids,
                       restricted_project_id=restricted_project_id) as indexd:
        for _ in dvc_objects:
            # add to indexd
            rc = indexd.save(_)
            yield rc


def to_remote(upload_method, dvc_objects, bucket_name, profile, dry_run, work_dir):
    """Upload committed files to remote."""
    # ['gen3', 's3', 's3-cp']
    if upload_method == 'gen3':
        writer = Gen3ClientRemoteWriter
    elif upload_method == 's3':
        writer = S3RemoteWriter
    else:
        logging.getLogger(__package__).info(f"No upload for {upload_method}")
        return

    with writer(work_dir=work_dir, log_file=f"logs/mock-remote-{upload_method}.log", remote=upload_method) as remote_writer:
        for _ in dvc_objects:
            remote_writer.save(_)
        remote_writer.commit(dry_run=dry_run, profile=profile, upload_path=pathlib.Path().cwd().resolve(), bucket_name=bucket_name)


def to_job(zip_file):
    """Upload zip files to submission"""
    with MockJobWriter(log_file="logs/mock-job.log") as job_writer:
        # TODO - add to remote
        job_writer.save(zip_file)
        time.sleep(3)
