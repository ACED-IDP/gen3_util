from typing import Callable

from . import Config


def test_from_tabular_default_resource(specimen: dict, to_tabular: Callable, from_tabular: Callable):
    """Should find and use default config."""

    # create tabular representation
    config = Config(default={'Specimen': ['receivedTime']})
    tabular = to_tabular(specimen, config, 'test-test')
    assert tabular == {'receivedTime': specimen['receivedTime']}

    # edit the tabular representation
    new_received_time = "2022-03-04T07:03:00Z"
    tabular['receivedTime'] = new_received_time

    # convert back to resource
    merged = from_tabular(tabular, config, 'test-test', specimen)

    # check that the edited tabular representation is reflected in the resource
    assert merged['receivedTime'] == new_received_time
    assert merged['receivedTime'] != specimen['receivedTime']

    # check that the merged resource is the same as the original resource, with the edited tabular representation reflected
    specimen['receivedTime'] = new_received_time
    assert merged == specimen


def test_from_tabular_deep_field_list(specimen: dict, to_tabular: Callable, specimen_fields: list[str], from_tabular: Callable):
    """Should preserve deep fields."""

    config = Config(default={'Specimen': specimen_fields})

    tabular = to_tabular(specimen, config, 'test-test')

    # edit the tabular representation
    tabular['subject_reference'] = "Subject/123"
    tabular['type_coding_0_code'] = "ABC"

    # convert back to resource
    merged = from_tabular(tabular, config, 'test-test', specimen)

    # check that the edited tabular representation is reflected in the resource
    assert tabular['subject_reference'] == merged['subject']['reference']
    assert tabular['subject_reference'] != specimen['subject']['reference']

    assert tabular['type_coding_0_code'] == merged['type']['coding'][0]['code']
    assert tabular['type_coding_0_code'] != specimen['type']['coding'][0]['code']
