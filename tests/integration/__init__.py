import pathlib
from click.testing import CliRunner, Result
import requests

from gen3_tracker.cli import cli
from gen3_tracker.config import ensure_auth, default
from gen3.query import Gen3Query


def run(
    runner: CliRunner,
    args: list[str],
    expected_output: list[str] = [],
    expected_exit_code: int = 0,
    expected_files: list[pathlib.Path] = [],
) -> Result:
    """Run a command and check the output, exit code and expected files."""
    if isinstance(args, str):
        args = args.split()
    if isinstance(expected_output, str):
        expected_output = expected_output.splitlines()
    if isinstance(expected_files, pathlib.Path):
        expected_files = [expected_files]
    expected_files = [pathlib.Path(_) for _ in expected_files]

    print("------------------------------------------------------------")
    print("g3t " + " ".join(args))
    result = runner.invoke(cli, args)
    print(result.stdout)
    assert (
        result.exit_code == expected_exit_code
    ), f"exit_code: {result.exit_code}, expected: {expected_exit_code}"
    for line in expected_output:
        assert (
            line in result.output
        ), f"output: {result.output}, expected: {expected_output}"
        print(f"{line} found in output.")
    for file in expected_files:
        assert file.exists(), f"{file} does not exist."
        print(f"{file} exists.")

    return result


def validate_document_in_grip(did: str, auth=None, project_id=None):
    """Simple query to validate a document in the grip graph."""
    if not auth:
        auth = ensure_auth(config=default())
    token = auth.get_access_token()
    result = requests.get(
        f"{auth.endpoint}/grip/writer/graphql/CALIPER/get-vertex/{did}/{project_id}",
        headers={"Authorization": f"bearer {token}"},
    ).json()
    assert "data" in result, f"Failed to query grip for {did} {result}"
    assert result["data"]["gid"] == did


def validate_document_in_elastic(did, auth):
    """Simple query to validate a document in elastic."""
    query = Gen3Query(auth)
    result = query.graphql_query(
        query_string="""
            query($filter:JSON) {
              file(filter:$filter) {
                id
              }
            }
        """,
        variables={"filter": {"AND": [{"IN": {"id": [did]}}]}},
    )
    print(result)
    assert result["data"]["file"][0]["id"] == did
