import uuid
import pytest
from click.testing import CliRunner


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for creating a CliRunner instance."""
    return CliRunner()


@pytest.fixture
def program() -> str:
    return "cbds"


@pytest.fixture
def project() -> str:
    project = uuid.uuid4().hex.replace('-', '_')
    return project


@pytest.fixture
def project_id(program, project) -> str:
    project_id = f"{program}-{project}"
    return project_id
