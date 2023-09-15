
import pytest
from gen3_util.meta.skeleton import create_skeleton


def test_meta_skeleton_full():
    """Ensure FHIR resources created from identifiers document reference -> observation, specimen, patient, task,research subject, research study"""
    resources = create_skeleton(
        metadata={
            'document_reference_id': 'ca71e316-48e1-11ee-be56-0242ac120002',
            'specimen_identifier': 'specimen-1',
            'patient_identifier': 'patient-1',
            'task_identifier': 'task-1',
            'observation_identifier': 'observation-1',
            'project_id': 'aced-foo'
        }
    )

    # check that we have the expected resources
    assert len(resources) == 7, f"expected 7 resources, got {len(resources)}"

    resource_types = [_.resource_type for _ in resources]
    for _ in ['DocumentReference', 'Observation', 'Patient', 'ResearchStudy', 'ResearchSubject', 'Specimen', 'Task']:
        assert _ in resource_types, f"expected {_} in {resource_types}"

    # check that we have the expected id for document reference
    document_reference = [_ for _ in resources if _.resource_type == 'DocumentReference'][0]
    assert document_reference.id == 'ca71e316-48e1-11ee-be56-0242ac120002', f"expected DocumentReference id to be ca71e316-48e1-11ee-be56-0242ac120002, got {resources[0].id}"

    # check that we have the expected identifiers
    expected_identifiers = {
        'Specimen': 'specimen-1',
        'Patient': 'patient-1',
        'Task': 'task-1',
        'Observation': 'observation-1',
    }
    for k, v in expected_identifiers.items():
        resource = [_ for _ in resources if _.resource_type == k][0]
        assert resource.identifier[0].value == v, f"expected {k} id to be {v}, got {resource.identifier[0].value}"
        assert resource.identifier[0].system == 'https://aced-idp.org/aced-foo', f"expected {k} system to be project, got {resource.identifier[0].system}"
        assert resource.identifier[0].use == 'official', f"expected {k} use to be official, got {resource.identifier[0].use}"

    # check that we have the expected references for task input and output

    task = [_ for _ in resources if _.resource_type == 'Task'][0]
    patient = [_ for _ in resources if _.resource_type == 'Patient'][0]
    specimen = [_ for _ in resources if _.resource_type == 'Specimen'][0]

    task_inputs = [_.valueReference.reference for _ in task.input]
    task_outputs = [_.valueReference.reference for _ in task.output]

    assert patient.id in ''.join(task_inputs), f"expected {patient.id} in {task_inputs}"
    assert specimen.id in ''.join(task_inputs), f"expected {specimen.id} in {task_inputs}"
    assert document_reference.id in ''.join(task_outputs), f"expected {document_reference.id} in {task_outputs}"

    # check that we have the expected references for research subject and research study

    research_subject = [_ for _ in resources if _.resource_type == 'ResearchSubject'][0]
    research_study = [_ for _ in resources if _.resource_type == 'ResearchStudy'][0]

    assert patient.id in research_subject.subject.reference, f"expected {patient.id} in {research_subject.subject.reference}"
    assert research_study.id in research_subject.study.reference, f"expected {research_study.id} in {research_subject.study.reference}"

    # check that we have the expected references for observation
    observation = [_ for _ in resources if _.resource_type == 'Observation'][0]
    assert patient.id in observation.subject.reference, f"expected {patient.id} in {observation.subject.reference}"
    assert specimen.id in observation.specimen.reference, f"expected {specimen.id} in {observation.specimen.reference}"

    # check that we have the expected references for document reference
    assert document_reference.subject,  "missing document_reference.subject"
    assert observation.id in document_reference.subject.reference, f"expected {observation.id} in {document_reference.subject.reference}"


def test_meta_skeleton_specimen():
    """Ensure FHIR resources created from identifiers for only document reference -> specimen"""
    resources = create_skeleton(
        metadata={
            'document_reference_id': 'ca71e316-48e1-11ee-be56-0242ac120002',
            'specimen_identifier': 'specimen-1',
            'patient_identifier': 'patient-1',
            'project_id': 'aced-foo'
        }
    )
    # check that we have the expected resources
    assert len(resources) == 5, f"expected 5 resources, got {len(resources)}"

    resource_types = [_.resource_type for _ in resources]
    for _ in ['DocumentReference', 'Patient', 'ResearchStudy', 'ResearchSubject', 'Specimen']:
        assert _ in resource_types, f"expected {_} in {resource_types}"

    specimen = [_ for _ in resources if _.resource_type == 'Specimen'][0]
    document_reference = [_ for _ in resources if _.resource_type == 'DocumentReference'][0]

    # check that we have the expected references for document reference
    assert document_reference.subject,  "missing document_reference.subject"
    assert specimen.id in document_reference.subject.reference, f"expected {specimen.id} in {document_reference.subject.reference}"


def test_meta_skeleton_patient():
    """Ensure FHIR resources created from identifiers for only document reference -> patient"""
    resources = create_skeleton(
        metadata={
            'document_reference_id': 'ca71e316-48e1-11ee-be56-0242ac120002',
            'patient_identifier': 'patient-1',
            'project_id': 'aced-foo'
        }
    )
    # check that we have the expected resources
    assert len(resources) == 4, f"expected 4 resources, got {len(resources)}"

    resource_types = [_.resource_type for _ in resources]
    for _ in ['DocumentReference', 'Patient', 'ResearchStudy', 'ResearchSubject']:
        assert _ in resource_types, f"expected {_} in {resource_types}"

    patient = [_ for _ in resources if _.resource_type == 'Patient'][0]
    document_reference = [_ for _ in resources if _.resource_type == 'DocumentReference'][0]

    # check that we have the expected references for document reference
    assert document_reference.subject,  "missing document_reference.subject"
    assert patient.id in document_reference.subject.reference, f"expected {patient.id} in {document_reference.subject.reference}"


def test_meta_skeleton_bad_parameters():
    """Ensure FHIR resources created from identifiers for only document reference -> patient"""
    _ = create_skeleton(
        metadata={
            'document_reference_id': 'ca71e316-48e1-11ee-be56-0242ac120002',
            'project_id': 'aced-foo'
        }
    )

    with pytest.raises(AssertionError):
        _ = create_skeleton(
            metadata={
                'project_id': 'aced-foo'
            }
        )

    with pytest.raises(AssertionError):
        _ = create_skeleton(
            metadata={
                'document_reference_id': 'ca71e316-48e1-11ee-be56-0242ac120002',
            }
        )


def test_meta_skeleton_minimal():
    """Ensure FHIR resources created from identifiers for only document reference and research study"""
    resources = create_skeleton(
        metadata={
            'document_reference_id': 'ca71e316-48e1-11ee-be56-0242ac120002',
            'project_id': 'aced-foo'
        }
    )

    # check that we have the expected resources
    assert len(resources) == 2, f"expected 2 resources, got {len(resources)}"

    resource_types = [_.resource_type for _ in resources]
    for _ in ['ResearchStudy', 'DocumentReference']:
        assert _ in resource_types, f"expected {_} in {resource_types}"

    # check that we have the expected references for research study

    research_study = [_ for _ in resources if _.resource_type == 'ResearchStudy'][0]
    document_reference = [_ for _ in resources if _.resource_type == 'DocumentReference'][0]

    # check that we have the expected references for document reference
    assert document_reference.subject,  "missing document_reference.subject"
    assert research_study.id in document_reference.subject.reference, f"expected {research_study.id} in {document_reference.subject.reference}"
