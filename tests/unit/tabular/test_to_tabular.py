
import pytest

from gen3_util.meta.tabular import Config, to_tabular


def test_to_tabular_default_resource(specimen: dict):
    """Should find and use default config."""
    config = Config(default={'Specimen': ['id']})
    tabular = to_tabular(specimen, config, 'test-test')
    assert tabular == {'id': specimen['id']}


def test_to_tabular_program_resource(specimen: dict):
    """Should find and use program config."""
    config = Config(default={'foo': []}, programs={'test': {'Specimen': ['id']}})
    tabular = to_tabular(specimen, config, 'test-test')
    assert tabular == {'id': specimen['id']}

    with pytest.raises(AssertionError):
        config = Config(default={'foo': []}, programs={'test': {'Specimen': ['id']}})
        to_tabular(specimen, config, 'non_existent_program-project')


def test_to_tabular_project_resource(specimen: dict):
    """Should find and use project config, degrade to program, then degrade to default."""
    config = Config(default={'Specimen': ['receivedTime']}, programs={'test': {'Specimen': ['id']}}, projects={'test-test': {'Specimen': ['id', 'status']}})

    tabular = to_tabular(specimen, config, 'test-test')
    assert tabular == {'id': specimen['id'], 'status': specimen['status']}

    tabular = to_tabular(specimen, config, 'test-non_existent_project')
    assert tabular == {'id': specimen['id']}

    tabular = to_tabular(specimen, config, 'non_existent_program-non_existent_project')
    assert tabular == {'receivedTime': specimen['receivedTime']}


def test_to_tabular_deep_field_list(specimen: dict, specimen_fields: list[str]):
    """Should preserve deep fields."""

    config = Config(default={'Specimen': specimen_fields})

    tabular = to_tabular(specimen, config, 'test-test')
    assert list(tabular.keys()) == specimen_fields
    assert tabular['subject_reference'] == specimen['subject']['reference']
    assert tabular['type_coding_0_code'] == specimen['type']['coding'][0]['code']
