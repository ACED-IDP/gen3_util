import csv
import json
import logging
import os
import pathlib
import shutil
import uuid
import zipfile
from datetime import datetime
from hashlib import md5
from typing import Mapping, Iterator, Dict, TextIO, Generator
from urllib.parse import urlparse

import orjson
import yaml
from fhir.resources.identifier import Identifier
from pydantic import BaseModel
from pydantic.json import pydantic_encoder

import io
import gzip
from gen3_util import Config, ACED_NAMESPACE

PROJECT_DIR = '.g3t'
PROJECT_DIRECTORIES = [PROJECT_DIR, 'META/']  # 'DATA/',
PROJECT_README = """
# Data Directory

Welcome to the data directory! This repository contains important data files for our project. Before you proceed, please take note of the following guidelines to ensure the security and integrity of our data.

## Important Note: Do Not Check in Protected Files

Some files in this directory are considered protected and contain sensitive information.

**DO NOT** check in or commit these protected files to the version control system (e.g., Git).

This is crucial to prevent unauthorized access and to comply with security and privacy policies.


## Usage Guidelines:

1. **Read-Only Access:** Unless you have explicit permission to modify or update the data, treat this directory as read-only.

2. **Data Integrity:** Ensure the integrity of the data by following proper procedures for reading, updating, and managing files.

3. **Security Awareness:** Be aware of the sensitivity of the data stored here and take necessary precautions to protect it from unauthorized access.


Thank you for your cooperation in maintaining the security and confidentiality of our data.
"""


def print_formatted(config: Config, output: Mapping) -> None:
    """Print the output, using configured output format"""

    if config.output.format == "yaml":
        print(yaml.dump(output, sort_keys=False))
    elif config.output.format == "json":
        print(
            orjson.dumps(
                output, default=pydantic_encoder, option=orjson.OPT_INDENT_2
            ).decode()
        )
    else:
        print(output)


def read_ndjson_file(path: str) -> Iterator[dict]:
    """Read ndjson file, load json line by line."""
    with _file_opener(path) as jsonfile:
        for l_ in jsonfile.readlines():
            yield orjson.loads(l_)


def read_json_file(path: str) -> Iterator[dict]:
    """Read ndjson file, load json line by line."""
    with _file_opener(path) as jsonfile:
        try:
            yield orjson.loads(jsonfile.read())
        except orjson.JSONDecodeError as e:
            logging.error(f"Error reading {path}: {e}")
            raise


def read_json(path: str) -> Iterator[dict]:
    """Read json or ndjson from file or zip."""
    if is_ndjson(path):
        _reader = read_ndjson_file
    else:
        _reader = read_json_file
    for _ in _reader(path):
        yield _


# TODO - unused, deprecate?
def read_tsv(path: str, delimiter="\t") -> Iterator[Dict]:
    """Read tsv file line by line."""
    with open(path) as tsv_file:
        reader = csv.DictReader(tsv_file, delimiter=delimiter)
        for row in reader:
            yield row


def read_yaml(path: str) -> Dict:
    """Read a yaml file."""
    with open(path, "r") as fp:
        return yaml.safe_load(fp.read())


def is_url(to_) -> bool:
    """Does the destination parameter describe an upload? ie have an url.scheme"""
    return len(urlparse(to_).scheme) > 0


def is_json_extension(name: str) -> bool:
    """Files we are interested in"""
    if name.endswith('json.gz'):
        return True
    if name.endswith('json'):
        return True
    return False


def is_ndjson(file_path: pathlib.Path) -> bool:
    """Open file, check if ndjson."""
    fp = _file_opener(file_path)
    try:
        with fp:
            for line in fp.readlines():
                orjson.loads(line)
                break
        return True
    except Exception as e:  # noqa
        return False


def _file_opener(file_path):
    """Open file appropriately."""
    if file_path.name.endswith('gz'):
        fp = io.TextIOWrapper(io.BufferedReader(gzip.GzipFile(file_path)))  # noqa
    else:
        fp = open(file_path, "rb")
    return fp


class EmitterContextManager:
    """Maintain file pointers to output directory."""

    def __init__(self, output_path: str, verbose=False, file_mode="w",
                 logger=logging.getLogger("EmitterContextManager")):
        """Ensure output_path exists, init emitter dict."""
        output_path = pathlib.Path(output_path)
        if not output_path.exists():
            output_path.mkdir(parents=True)
        assert output_path.is_dir(), f"{output_path} not a directory?"

        self.output_path = output_path
        """destination directory"""
        self.emitters = {}
        """open file pointers"""
        self.verbose = verbose
        """log activity"""
        self.file_mode = file_mode
        """mode for file opens"""
        self.logger = logger

    def __enter__(self):
        """Ensure output_path exists, init emitter dict.
        """
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Close all open files."""
        for _ in self.emitters.values():
            _.close()
            if self.verbose:
                self.logger.info(f"closed {_.name}")

    def emit(self, name: str) -> TextIO:
        """Maintain a hash of open files."""
        if name not in self.emitters:
            self.emitters[name] = open(self.output_path / f"{name}.ndjson", self.file_mode)
            if self.verbose:
                self.logger.info(f"opened {self.emitters[name].name}")

            # If the user already has metadata in the resource file but wants to add
            # another file the the first line will create a 2nd record on the same line causing the commit step to error
            if os.path.getsize(self.emitters[name].name) > 0:
                self.emitters[name].seek(self.emitters[name].tell() - 1, os.SEEK_SET)
                last_char = self.emitters[name].read()
                if last_char == "}":
                    self.emitters[name].write("\n")
        return self.emitters[name]


def validate_project_id(project_id) -> list[str]:
    """Ensure that the project_id is valid"""
    msgs = []
    if not project_id:
        msgs.append("project_id is missing")
    assert project_id.count('-') in [0, 1], f"{project_id} should have a single '-' delimiter."
    return msgs


def validate_email(email) -> list[str]:
    """Ensure that the email is valid"""
    msgs = []
    if not email:
        msgs.append("email is missing")
    if not email.count('@') == 1:
        msgs.append(f"{email} should have a single '@' delimiter.")
    try:
        from email_validator import validate_email as email_validator_validate, EmailNotValidError
        email_validator_validate(email)
    except EmailNotValidError as e:
        msgs.append(f"{email} is not a valid email address. {e}")
    return msgs


def to_resource_path(project_id):
    """Canonical conversion of project_id to resource path."""
    if '-' not in project_id:
        return project_id
    _ = project_id.split('-')
    return f"/programs/{_[0]}/projects/{_[1]}"


def unzip_collapse(zip_file, extract_to):
    """Unzip a file, collapse the directory structure."""
    c = 0
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_info in zip_ref.infolist():
            if file_info.is_dir():
                continue
            filename = os.path.basename(file_info.filename)
            extracted_path = os.path.join(extract_to, filename)
            with zip_ref.open(file_info.filename) as source, open(extracted_path, 'wb') as target:
                shutil.copyfileobj(source, target)
            c += 1
    assert c > 0, f"no files extracted from {zip_file}"


def calc_md5(file_path, md5_hash):
    """Calculate the md5 file, sort keys of json files."""
    read_binary = False
    with open(file_path, "r") as f:
        if file_path.name.endswith('ndjson'):
            for line in f.readlines():
                line = orjson.loads(line)
                md5_hash.update(orjson.dumps(line, option=orjson.OPT_SORT_KEYS))
        elif file_path.name.endswith('json'):
            data = orjson.load(f)
            md5_hash.update(orjson.dumps(data, option=orjson.OPT_SORT_KEYS))
        else:
            read_binary = True
    if read_binary:
        with open(file_path, "rb") as f:
            for line in f.readlines():
                md5_hash.update(line)
    return md5_hash


def identifier_to_string(identifier: list[Identifier]) -> str:
    """Return query parameter for identifier."""
    if identifier and not isinstance(identifier, list):
        identifier = [identifier]
    assert identifier and len(identifier) > 0, "identifier required"
    official = [_ for _ in identifier if _.use == 'official']
    if len(official) > 0:
        _ = official[0]
    else:
        _ = identifier[0]

    if _.system:
        return f"{_.system}|{_.value}"
    return _.value


def create_id(resource, project_id) -> str:
    """Return id from identifier and project_id."""
    assert resource, "resource required"
    assert project_id, "project_id required"
    identifier_string = identifier_to_string(resource.identifier)
    return str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/{resource.resource_type}/{identifier_string}"))


class Commit(BaseModel):
    """A commit."""
    commit_id: str = None
    """The commit id."""
    object_id: str = None
    """The metadata file object_id."""
    message: str = None
    """The commit message."""
    resource_counts: dict = None
    """The resource counts of meta in this commit."""
    exceptions: list = None
    """The exceptions."""
    logs: list = None
    """The logs."""
    path: pathlib.Path = None
    """The path to the commit directory."""
    manifest_sqlite_path: pathlib.Path = None
    """The path to the manifest file."""
    meta_path: pathlib.Path = None
    """The path to the meta zip file."""


class Push(BaseModel):
    """A list of commits."""

    config: Config
    """The config."""

    commits: list[Commit] = []
    """A list of commits in this push."""

    published_timestamp: datetime = None
    """When the push was published."""

    published_job: dict = None

    def model_dump(self):
        """Dump the model.

         temporary until we switch to pydantic2
        """
        _ = self.model_dump_json(exclude={'config'})
        return json.loads(_)

    def pending_commits(self):
        """A list of commits yet to be pushed."""
        pending = []
        commits_dir = self.config.state_dir / self.config.gen3.project_id / 'commits'
        pending_path = commits_dir / 'pending.ndjson'
        if not pending_path.exists():
            return pending
        try:
            for _ in read_ndjson_file(pending_path):
                commit_dict = orjson.loads(open(commits_dir / _['commit_id'] / 'resource.json').read())
                commit = Commit(**commit_dict)

                commit.manifest_sqlite_path = commits_dir / _['commit_id'] / 'manifest.sqlite'
                commit.meta_path = commits_dir / _['commit_id'] / 'meta.zip'

                pending.append(commit)
            return pending
        except FileNotFoundError as e:
            print(f"No pending commits found in {pending_path} {e}")

    def pending_meta_index(self) -> list[dict]:
        """Index of pending meta files {id: resourceType}."""
        commits_dir = self.config.state_dir / self.config.gen3.project_id / 'commits'
        pending_path = commits_dir / 'pending.ndjson'
        pending = []
        if not pending_path.exists():
            return pending
        for _ in read_ndjson_file(pending_path):
            with open(commits_dir / _['commit_id'] / 'meta-index.ndjson') as fp:
                for line in fp.readlines():
                    pending.append(orjson.loads(line))
        return pending


def dict_md5(resource: dict) -> str:
    """Return the md5 of the dict."""
    return md5(orjson.dumps(resource, option=orjson.OPT_SORT_KEYS)).hexdigest()


def write_meta_index(index_path: pathlib.Path, source_path: pathlib.Path) -> pathlib.Path:
    """Write an index of ids and hashes from a path containing ndjson files.
    """
    index_path = index_path / 'meta-index.ndjson'
    with open(index_path, 'w') as fp:
        for _ in sorted(source_path.glob("*.ndjson")):
            for resource in read_ndjson_file(_):
                _ = {'id': resource['id'], 'resourceType': resource['resourceType'], 'md5': dict_md5(resource)}
                fp.write(orjson.dumps(_).decode())
                fp.write('\n')
    return index_path


def read_meta_index(index_path: pathlib.Path) -> Generator[dict, None, None]:
    """Read an index of ids and hashes from a path containing ndjson files.
    """
    index_path = index_path / 'meta-index.ndjson'
    if not index_path.exists():
        return
    with open(index_path, 'r') as fp:
        for line in fp.readlines():
            yield orjson.loads(line)


def to_metadata_dict(project_id=None, is_metadata=None, is_snapshot=None, md5=None, observation=None, patient=None, specimen=None, task=None):
    """Create metadata dict from parameters"""
    _ = {}
    if project_id:
        _['project_id'] = project_id
    if specimen:
        _['specimen_id'] = specimen
    if patient:
        _['patient_id'] = patient
    if task:
        _['task_id'] = task
    if observation:
        _['observation_id'] = observation
    if md5:
        _['md5'] = md5
    if is_metadata:
        _['is_metadata'] = is_metadata
    if is_snapshot:
        _['is_snapshot'] = is_snapshot
    return _
