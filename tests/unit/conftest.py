from typing import List

import pytest
from gen3.submission import Gen3Submission

from gen3_util.config import ensure_auth


@pytest.fixture
def python_source_directories() -> List[str]:
    """Directories to scan with flake8."""
    return ["gen3_util", "tests"]


@pytest.fixture
def custom_config_path() -> str:
    """User specified config path"""
    return 'tests/fixtures/custom_config/config.yaml'


@pytest.fixture
def submission_client() -> str:
    """Gen3Submission client"""
    auth = ensure_auth(profile='development')
    return Gen3Submission(auth_provider=auth)


@pytest.fixture
def test_files_directory() -> str:
    """Directory containing test files"""
    return 'tests/fixtures/add_files_to_study'
