import pathlib

import pytest
from gen3_util.files.manifest import put


def test_put(test_files_directory, pattern='**/*'):
    """Test manifest put."""
    input_path = pathlib.Path(test_files_directory)
    project_id = 'test-project'
    for file in input_path.glob(pattern):
        if any([file.is_dir(), file.is_symlink()]):
            continue
        _ = put(file_name=str(file), project_id=project_id, config=None, hash=None)
        assert _['object_id'], f"object_id is missing for {file}"
        assert _['md5'], f"md5 is missing for {file}"
        assert _['mime_type'], f"mime_type is missing for {file}"
        assert _['size'], f"file_name is missing for {file}"
        assert _['file_name'], f"file_name is missing for {file}"

def test_import_missing_etag(test_files_directory, pattern='**/*'):
    """Test manifest put."""
    input_path = pathlib.Path(test_files_directory)
    project_id = 'test-project'
    for file in input_path.glob(pattern):
        if any([file.is_dir(), file.is_symlink()]):
            continue
        with pytest.raises(Exception) as excinfo:
            _ = put(file_name=str(file), project_id=project_id, config=None, hash=None, hash_type='etag')
            assert "etag value not provided" in str(excinfo.value)

def test_import(test_files_directory, pattern='**/*'):
    """Test manifest put."""
    input_path = pathlib.Path(test_files_directory)
    project_id = 'test-project'
    for file in input_path.glob(pattern):
        if any([file.is_dir(), file.is_symlink()]):
            continue
        _ = put(file_name=str(file), project_id=project_id, config=None, hash='123-a', hash_type='etag')
        assert _['object_id'], f"object_id is missing for {file}"
        assert _['etag'] == '123-a', f"etag is missing for {file}"
        assert _['mime_type'], f"mime_type is missing for {file}"
        assert _['size'], f"file_name is missing for {file}"
        assert _['file_name'], f"file_name is missing for {file}"
