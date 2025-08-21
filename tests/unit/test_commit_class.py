import pytest

from gen3_tracker.common import Commit


@pytest.fixture
def object_id():
    return "1234-1234-1234-1234"


@pytest.fixture
def valid_str_path():
    return "valid/path"


@pytest.fixture()
def valid_commit(object_id, valid_str_path):
    return Commit(
        object_id=object_id,
        message=valid_str_path,
        meta_path=valid_str_path,
        commit_id=object_id,
    )


@pytest.fixture()
def invalid_commit(object_id, valid_str_path):
    return Commit(
        object_id=object_id,
        message=valid_str_path,
        # note that no meta_path is defined
        commit_id=object_id,
    )


def test_commit_model_dump_no_warnings_on_model_dump(recwarn, valid_commit):
    # no record of warnings if `path` is not defined
    valid_commit.model_dump()
    assert len(recwarn) == 0

    # no record of warnings if `path` is set to None
    commit = valid_commit
    commit.path = None
    commit.model_dump()
    assert len(recwarn) == 0


def test_define_commit_encounters_pydantic_serialization_warning(invalid_commit):
    # look for Pydantic warning message
    with pytest.warns(UserWarning) as warning:
        invalid_commit.model_dump()
        assert any(
            "PydanticSerializationUnexpectedValue" in str(line.message)
            for line in warning
        ), "Expected no PydanticSerializationUnexpectedValue warning when meta_path is not set"
