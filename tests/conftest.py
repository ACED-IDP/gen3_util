import pytest

from pathlib import Path

@pytest.fixture()
def data_path():
    return Path(__file__).parent / "fixtures"