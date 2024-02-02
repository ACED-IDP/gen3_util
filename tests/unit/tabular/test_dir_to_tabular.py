import pathlib

from gen3_util.meta.validator import validate


def test_dir_to_tabular(tabular_path: pathlib.Path, tmp_path: pathlib.Path):
    """Should convert to and from tabular."""
    from gen3_util.meta.tabular import transform_dir_to_tabular, transform_dir_from_tabular

    parse_result = validate(directory_path=tabular_path, config=None)
    for _ in parse_result.exceptions:
        print(_)
    assert not parse_result.exceptions, "Should have no exceptions"
    for _ in transform_dir_to_tabular(tabular_path, tmp_path, file_type='tsv'):
        print(_)
    assert (tmp_path / "Patient.tsv").exists()
    assert (tmp_path / "Patient.tsv").is_file()

    for _ in transform_dir_from_tabular(tmp_path, tmp_path):
        print(_)
    assert (tmp_path / "Patient.ndjson").exists()
    assert (tmp_path / "Patient.ndjson").is_file()

    parse_result = validate(directory_path=tmp_path, config=None)
    for _ in parse_result.exceptions:
        print(_)
    assert not parse_result.exceptions, "Should have no exceptions"
