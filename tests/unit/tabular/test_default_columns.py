import pytest


def test_default_columns(default_columns, observation, specimen, patient):
    """Ensure we can retrieve default columns for a resource type."""
    for resource in [observation, specimen, patient]:

        column_names = default_columns(resource)
        resource_type = resource['resourceType']

        # check returned list
        assert column_names, f"Should return value for {resource_type}"
        assert isinstance(column_names, list), f"Should return list for {resource_type}"
        assert all([isinstance(_, str) for _ in column_names]), f"Should return list of strings for {resource_type}"

        # check specific columns
        if 'id' in resource:
            assert 'id' in column_names, f"Should return id for {resource_type}"

        meta_columns = [_ for _ in column_names if _.startswith('meta')]
        assert not meta_columns, f"Should not return meta columns for {resource_type}, got {meta_columns}"

        assert column_names[0] == 'resourceType', f"Should return resourceType as first column for {resource_type}"
        assert column_names[1] == 'id', f"Should return id as second column for {resource_type}"


def test_default_columns_observation(default_columns, observation):
    """Check specific columns for Observation"""
    column_names = default_columns(observation)
    expected_observation_column_names = [
        # all should start with resourceType, id
        'resourceType', 'id',
        # then scalars
        'effectiveDateTime', 'issued', 'status',
        # then nested
        'category_0_coding_0_code', 'category_0_coding_0_display', 'category_0_coding_0_system',
        'code_coding_0_code', 'code_coding_0_display', 'code_coding_0_system', 'code_text',
        'valueQuantity_code', 'valueQuantity_system', 'valueQuantity_unit', 'valueQuantity_value',
        # then references
        'encounter_reference', 'subject_reference'
    ]

    assert column_names == expected_observation_column_names, \
        f"Should return expected columns for Observation, got {column_names}"


def test_default_columns_specimen(default_columns, specimen):
    """Check specific columns for Specimen"""
    column_names = default_columns(specimen)
    expected_specimen_column_names = [
        # all should start with resourceType, id
        'resourceType', 'id',
        # then scalars
        'receivedTime', 'status',
        # then nested
        'accessionIdentifier_system', 'accessionIdentifier_value', 'collection_bodySite_concept_coding_0_code',
        'collection_bodySite_concept_coding_0_display', 'collection_bodySite_concept_coding_0_system',
        'collection_collectedDateTime', 'collection_method_coding_0_code', 'collection_method_coding_0_system',
        'collection_quantity_unit', 'collection_quantity_value', 'container_0_specimenQuantity_unit',
        'container_0_specimenQuantity_value', 'identifier_0_system', 'identifier_0_value', 'note_0_text',
        'processing_0_additive_0_display', 'processing_0_description', 'processing_0_method_coding_0_code',
        'processing_0_method_coding_0_system', 'processing_0_timeDateTime', 'subject_display', 'text_div',
        'text_status', 'type_coding_0_code', 'type_coding_0_display', 'type_coding_0_system',
        # then references
        'collection_collector_reference', 'container_0_device_reference', 'processing_0_additive_0_reference', 'request_0_reference', 'subject_reference'
    ]
    assert column_names == expected_specimen_column_names, \
        f"Should return expected columns for Specimen, got {column_names}"


def test_default_columns_patient(default_columns, patient):
    """Check specific columns for Specimen"""
    column_names = default_columns(patient)
    expected_patient_column_names = [
        'resourceType', 'id',
        'birthDate', 'deceasedDateTime', 'gender', 'multipleBirthBoolean',
        'address_0_city', 'address_0_country', 'address_0_extension_0_extension_0_url',
        'address_0_extension_0_extension_0_valueDecimal', 'address_0_extension_0_extension_1_url',
        'address_0_extension_0_extension_1_valueDecimal', 'address_0_extension_0_url', 'address_0_line_0',
        'address_0_state',
        'communication_0_language_coding_0_code', 'communication_0_language_coding_0_display',
        'communication_0_language_coding_0_system', 'communication_0_language_text',
        'extension_0_extension_0_url', 'extension_0_extension_0_valueCoding_code',
        'extension_0_extension_0_valueCoding_display', 'extension_0_extension_0_valueCoding_system',
        'extension_0_extension_1_url', 'extension_0_extension_1_valueString', 'extension_0_url',
        'extension_1_extension_0_url', 'extension_1_extension_0_valueCoding_code',
        'extension_1_extension_0_valueCoding_display', 'extension_1_extension_0_valueCoding_system',
        'extension_1_extension_1_url', 'extension_1_extension_1_valueString', 'extension_1_url',
        'extension_2_url', 'extension_2_valueString',
        'extension_3_url', 'extension_3_valueCode',
        'extension_4_url', 'extension_4_valueAddress_city', 'extension_4_valueAddress_country',
        'extension_4_valueAddress_state',
        'extension_5_url', 'extension_5_valueDecimal',
        'extension_6_url', 'extension_6_valueDecimal',
        'identifier_0_system', 'identifier_0_value',
        'identifier_1_system', 'identifier_1_type_coding_0_code', 'identifier_1_type_coding_0_display',
        'identifier_1_type_coding_0_system', 'identifier_1_type_text', 'identifier_1_value',
        'identifier_2_system', 'identifier_2_type_coding_0_code', 'identifier_2_type_coding_0_display',
        'identifier_2_type_coding_0_system', 'identifier_2_type_text', 'identifier_2_value',
        'identifier_3_system', 'identifier_3_type_coding_0_code', 'identifier_3_type_coding_0_display',
        'identifier_3_type_coding_0_system', 'identifier_3_type_text', 'identifier_3_value',
        'identifier_4_system', 'identifier_4_type_coding_0_code', 'identifier_4_type_coding_0_display',
        'identifier_4_type_coding_0_system', 'identifier_4_type_text', 'identifier_4_value',
        'maritalStatus_coding_0_code', 'maritalStatus_coding_0_display', 'maritalStatus_coding_0_system',
        'maritalStatus_text',
        'name_0_family', 'name_0_given_0', 'name_0_prefix_0', 'name_0_use',
        'telecom_0_system', 'telecom_0_use', 'telecom_0_value',
        'text_div', 'text_status'
    ]

    assert column_names == expected_patient_column_names, \
        f"Should return expected columns for Patient, got {column_names}"


def test_not_fhir(default_columns):
    """Expected error if missing resourceType."""
    # error if no resourceType
    with pytest.raises(KeyError):
        default_columns({})
