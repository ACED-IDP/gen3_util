import csv
import logging
import pathlib
from typing import Mapping, Iterator, Dict, TextIO
from urllib.parse import urlparse

import orjson
import yaml
from pydantic.json import pydantic_encoder
import io
import gzip
from gen3_util.config import Config


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
        return self.emitters[name]


def validate_project_id(project_id) -> list[str]:
    """Ensure that the project_id is valid"""
    msgs = []
    if not project_id:
        msgs.append("project_id is missing")
    if not project_id.count('-') == 1:
        msgs.append(f"{project_id} should have a single '-' delimiter.")
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
