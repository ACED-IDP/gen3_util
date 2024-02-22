
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


def test_mixed_observations(mixed_observations: list[dict]):
    """Should handle multiple resources."""
    observation_fields = [
        'identifier_0_value',
        'valueInteger',
        'valueQuantity_value',
        'valueString',
        'valueCodeableConcept_coding_0_code',

        # 'category_0_coding_0_code',
        # 'category_0_coding_0_display',
        # 'category_0_coding_0_system',
        # 'category_0_text',
        'code_coding_0_code',
        'code_coding_0_display',
        'code_coding_0_system',
        'code_coding_1_code',
        'code_coding_1_display',
        'code_coding_1_system',
        'code_text',
        # 'focus_0_reference',
        'id',
        # 'identifier_0_system',
        # 'identifier_0_use',
        # 'identifier_0_value',
        # 'resourceType',
        # 'status',
        # 'subject_reference',
    ]

    config = Config(default={'Observation': observation_fields})
    for observation in mixed_observations:
        tabular = to_tabular(observation, config, 'test-test')
        assert list(tabular.keys()) == observation_fields
        assert tabular['valueInteger'] == observation.get('valueInteger', None)
        assert tabular['valueQuantity_value'] == observation.get('valueQuantity', {}).get('value', None)
        assert tabular['valueString'] == observation.get('valueString', None)
        assert tabular['valueCodeableConcept_coding_0_code'] == observation.get('valueCodeableConcept', {}).get('coding', [{}])[0].get('code', None)
        print(tabular['id'], tabular['valueInteger'], tabular['valueQuantity_value'], tabular['valueString'], tabular['valueCodeableConcept_coding_0_code'])
        assert any([tabular['valueInteger'], tabular['valueQuantity_value'], tabular['valueString'], tabular['valueCodeableConcept_coding_0_code']])
