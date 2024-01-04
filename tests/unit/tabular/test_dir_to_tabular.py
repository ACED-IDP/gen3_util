import pathlib


def test_dir_to_tabular(meta_path: pathlib.Path, tmp_path: pathlib.Path):
    """Should convert to and from tabular."""
    from gen3_util.meta.tabular import transform_dir_to_tabular, transform_dir_from_tabular

    for _ in transform_dir_to_tabular(meta_path, tmp_path, file_type='tsv'):
        print(_)
    assert (tmp_path / "Patient.tsv").exists()
    assert (tmp_path / "Patient.tsv").is_file()

    for _ in transform_dir_from_tabular(tmp_path, tmp_path):
        print(_)
    assert (tmp_path / "Patient.ndjson").exists()
    assert (tmp_path / "Patient.ndjson").is_file()
