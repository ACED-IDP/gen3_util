import pathlib
from click.testing import CliRunner, Result

from gen3_tracker.cli import cli
from gen3_tracker.config import ensure_auth, default
from gen3.submission import Gen3Submission
from gen3.query import Gen3Query


def run(runner: CliRunner, args: list[str], expected_output: list[str] = [], expected_exit_code: int = 0, expected_files: list[pathlib.Path] = []) -> Result:
    """Run a command and check the output, exit code and expected files."""
    if isinstance(args, str):
        args = args.split()
    if isinstance(expected_output, str):
        expected_output = expected_output.splitlines()
    if isinstance(expected_files, pathlib.Path):
        expected_files = [expected_files]
    expected_files = [pathlib.Path(_) for _ in expected_files]

    print('------------------------------------------------------------')
    print("g3t " + " ".join(args))
    result = runner.invoke(cli, args)
    print(result.stdout)
    assert result.exit_code == expected_exit_code, f"exit_code: {result.exit_code}, expected: {expected_exit_code}"
    for line in expected_output:
        assert line in result.output, f"output: {result.output}, expected: {expected_output}"
        print(f"{line} found in output.")
    for file in expected_files:
        assert file.exists(), f"{file} does not exist."
        print(f"{file} exists.")

    return result


def validate_document_in_psql_graph(did: str, auth=None):
    """Simple query to validate a document in the graph."""
    if not auth:
        auth = ensure_auth(config=default())
    gen3_submission = Gen3Submission(auth)
    result = gen3_submission.query(query_txt="""
        {
            document_reference(id:"DID") {
                id
            }
        }
    """.replace("DID", did))
    print(result)
    assert result['data']['document_reference'][0]['id'] == did


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
        variables={"filter": {"AND": [{"IN": {"id": [did]}}]}}
    )
    print(result)
    assert result['data']['file'][0]['id'] == did
