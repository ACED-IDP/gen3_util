import pathlib

from gen3_util.meta import directory_reader
from gen3_util.meta.validator import validate


def test_validate_directory_files(directory_path=pathlib.Path('tests/fixtures/valid-files')):
    """Ensure valid json rendered from files."""
    results = validate(None, directory_path)
    assert len(results.exceptions) == 0, f"Did not expect exceptions {results.exceptions}"
    assert sum([_ for _ in results.resources['summary'].values()]) == 5, f"Expected 5 resources {results.resources}"


def test_validate_directory_zips(directory_path=pathlib.Path('tests/fixtures/valid-zips')):
    """Ensure valid json rendered from gz."""
    results = validate(None, directory_path)
    assert len(results.exceptions) == 0, f"Did not expect exceptions {results.exceptions}"
    assert sum([_ for _ in results.resources['summary'].values()]) == 5, f"Expected 5 resources {results.resources}"


def test_validate_invalid_files(directory_path=pathlib.Path('tests/fixtures/invalid-files')):
    """Ensure invalid json is captured."""
    results = validate(None, directory_path)
    assert len(results.exceptions) == 3, f"Expected exceptions {results.exceptions}"


def test_validate_pattern(directory_path=pathlib.Path('tests/fixtures/valid-files'), pattern="bundle.json"):
    for result in directory_reader(directory_path, pattern):
        print(result)
        assert result.offset is not None, "Expected offset"
        assert result.exception is None, "Unexpected exception"
        assert result.resource, ("Expected resource", result)
