import json
import os
import pathlib

from tests.integration import run
from click.testing import CliRunner
import pytest


SHOULD_SUCCEED = [
    "--debug add s3://s3-bucket/s3-object --size 1 --modified 2024-05-05T07:26:29-0700 --md5 acbd18db4cc2f85cedef654fccc4a4d8",
    "--debug add https://example.com/http-bucket/http-object --size 1 --modified 2024-05-05T07:26:29-0700 --sha1 2ef7bde608ce5404e97d5f042f95f89f1c232871",
    "--debug add azure://azure-bucket/azure-object --size 1 --modified 2024-05-05T07:26:29-0700 --sha512 3ba2942ed1d05551d4360a2a7bb6298c2359061dc07b368949bd3fb7feca3344221257672d772ce456075b7cfa50fd7ce41eaefe529d056bf23dd665de668b78",
    "--debug add gs://gs-bucket/gs-object --size 1 --modified 2024-05-05T07:26:29-0700 --etag acbd18db4cc2f85cedef654fccc4a4d8-3",
]

EXPECTED_MANIFEST_PATHS = [
    "MANIFEST/s3-bucket/s3-object.dvc",
    "MANIFEST/http-bucket/http-object.dvc",
    "MANIFEST/azure-bucket/azure-object.dvc",
    "MANIFEST/gs-bucket/gs-object.dvc",
]

SHOULD_FAIL = [
    # unsupported scheme
    "--debug add foo://s3-bucket/s3-object --size 1 --modified 2024-05-05T07:26:29-0700 --md5 acbd18db4cc2f85cedef654fccc4a4d8",
    # no size
    "--debug add s3://s3-bucket/s3-object  --modified 2024-05-05T07:26:29-0700 --md5 acbd18db4cc2f85cedef654fccc4a4d8",
    # negative size
    "--debug add https://example.com/http-bucket/http-object --size -1 --modified 2024-05-05T07:26:29-0700 --sha1 2ef7bde608ce5404e97d5f042f95f89f1c232871",
    # invalid modified
    "--debug add azure://azure-bucket/azure-object --size 1 --modified XXX --sha512 3ba2942ed1d05551d4360a2a7bb6298c2359061dc07b368949bd3fb7feca3344221257672d772ce456075b7cfa50fd7ce41eaefe529d056bf23dd665de668b78",
    # invalid hash
    "--debug add gs://gs-bucket/gs-object --size 1 --modified 2024-05-05T07:26:29-0700 --etag foo",
]


def test_bucket_import(runner: CliRunner, project_id, tmpdir) -> None:
    """Test import from sources other than filesystem."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(pathlib.Path.cwd())

    print(project_id)

    run(runner, ["--debug", "init", project_id, "--approve", "--no-server"],
        expected_files=[".g3t", ".git"])

    for _ in SHOULD_SUCCEED:
        run(runner, _.split())

    for _ in EXPECTED_MANIFEST_PATHS:
        _ = pathlib.Path(_)
        assert _.exists(), f"{_} does not exist."

    for _ in SHOULD_FAIL:
        with pytest.raises(Exception):
            run(runner, _.split())

    # test the ls command
    result = run(runner, ["--debug", "--format", "json",  "ls"])
    listing = json.loads(result.stdout)

    for _ in ['bucket', 'committed', 'uncommitted']:
        assert _ in listing

    # files should appear in uncommitted
    assert len(listing['uncommitted']) == len(SHOULD_SUCCEED)

    # commit the changes
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # test the ls command, should now be in committed
    result = run(runner, ["--debug", "--format", "json",  "ls"])
    listing = json.loads(result.stdout)
    assert len(listing['committed']) == len(SHOULD_SUCCEED)

    # test the ls filter
    for _ in EXPECTED_MANIFEST_PATHS:
        bucket_name = _.split('/')[1]
        result = run(runner, ["--debug", "--format", "json",  "ls", bucket_name])
        listing = json.loads(result.stdout)
        assert len(listing['committed']) == 1
