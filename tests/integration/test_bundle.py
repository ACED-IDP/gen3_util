import json
import os
import pathlib

from fhir.resources.bundle import Bundle

from g3t.common import read_ndjson_file
from tests.integration import run
from click.testing import CliRunner
import pytest


CHANGE_PATIENT = [
    "--debug add s3://s3-bucket/p1-object.txt --size 1 --modified 2024-05-05T07:26:29-0700 --md5 acbd18db4cc2f85cedef654fccc4a4d8 --patient P1",
    "--debug meta init",
    "--debug commit -am \"initial commit\"",
    "--debug add s3://s3-bucket/p1-object.txt --size 1 --modified 2024-05-05T07:26:29-0700 --md5 acbd18db4cc2f85cedef654fccc4a4d8 --patient P1-prime",
    "--debug meta init",
    "--debug commit -am \"prime commit\"",
]


def test_bundle(runner: CliRunner, project_id, tmpdir) -> None:
    """Ensure bundle gets created when `meta init` orphans records.."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(pathlib.Path.cwd())

    print(project_id)

    run(runner, ["--debug", "init", project_id, "--approve", "--no-server"],
        expected_files=[".g3t", ".git"])

    for _ in CHANGE_PATIENT:
        run(runner, _.split())

    for _ in read_ndjson_file(pathlib.Path("META/Bundle.ndjson")):
        bundle = Bundle(**_)
        break  # only one bundle

    assert len(bundle.entry) == 2, "Only two entries are expected."

    methods = [_.request.method for _ in bundle.entry]
    assert all([_ == "DELETE" for _ in methods]), "Only DELETE method is expected."

    urls = [_.request.url for _ in bundle.entry]
    assert any([_.startswith('Patient') for _ in urls]), "Expected to delete a Patient."
    assert any([_.startswith('ResearchSubject') for _ in urls]), "Expected to delete a ResearchSubject."
