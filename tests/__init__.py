import pathlib

from click.testing import CliRunner, Result

from gen3_tracker.cli import cli


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
    print("result.stdout", result.stdout)
    print("result.output", result.output)
    print("result.exception", result.exception)
    print("CWD", pathlib.Path.cwd())
    assert (
        result.exit_code == expected_exit_code
    ), f"g3t {' '.join(args)} exit_code: {result.exit_code}, expected: {expected_exit_code}"
    for line in expected_output:
        assert (
            line in result.output
        ), f"output: {result.output}, expected: {expected_output}"
        print(f"{line} found in output.")
    for file in expected_files:
        assert file.exists(), f"{file} does not exist."
        print(f"{file} exists.")

    return result
