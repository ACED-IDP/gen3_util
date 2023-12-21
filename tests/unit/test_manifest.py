import pathlib
from gen3_util.files.manifest import put


def test_put(test_files_directory, pattern='**/*'):
    """Test manifest put."""
    input_path = pathlib.Path(test_files_directory)
    project_id = 'test-project'
    for file in input_path.glob(pattern):
        if any([file.is_dir(), file.is_symlink()]):
            continue
        _ = put(file_name=str(file), project_id=project_id, config=None, md5=None)
        assert _['object_id'], f"object_id is missing for {file}"
        assert _['md5'], f"md5 is missing for {file}"
        assert _['mime_type'], f"mime_type is missing for {file}"
        assert _['size'], f"file_name is missing for {file}"
        assert _['file_name'], f"file_name is missing for {file}"
