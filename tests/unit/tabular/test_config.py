
from gen3_util.meta.tabular import Config


def test_config_missing_resource():
    """Should return default."""
    default = {'foo': ['a', 'b']}
    config = Config(default=default)
    assert config.get_config('test-test') == default


def test_config_program_resource():
    """Should find and use program config."""
    programs = {'test': {'Specimen': ['id']}}
    default = {'foo': []}
    config = Config(default=default, programs=programs)

    assert config.get_config('test-test') == programs['test']
    assert config.get_config('non_existent_program-project') == default


def test_config_project_resource():
    """Should find and use project config, degrade to program, then degrade to default."""
    default = {'Specimen': ['receivedTime']}
    programs = {'test': {'Specimen': ['id']}}
    projects = {'test-test': {'Specimen': ['id', 'status']}}
    config = Config(default=default, programs=programs, projects=projects)

    assert config.get_config('test-test') == projects['test-test']
    assert config.get_config('test-non_existent_project') == programs['test']
    assert config.get_config('non_existent_program-non_existent_project') == default
