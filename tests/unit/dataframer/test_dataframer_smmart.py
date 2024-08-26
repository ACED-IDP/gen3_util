import json
import os
import pathlib
import tempfile

import inflection
import pytest

from gen3_tracker.common import read_ndjson_file
from gen3_tracker.meta.dataframer import LocalFHIRDatabase
from tests.unit.dataframer.dataframer import SimplifiedResource


@pytest.fixture()
def simplified_smmart_resources():
    return {
        'DocumentReference/9ae7e542-767f-4b03-a854-7ceed17152cb': {'identifier': '9ae7e542-767f-4b03-a854-7ceed17152cb',
                                                                   'resourceType': 'DocumentReference',
                                                                   'id': '9ae7e542-767f-4b03-a854-7ceed17152cb',
                                                                   'status': 'current', 'docStatus': 'final',
                                                                   'date': '2024-08-21T10:53:00+00:00'},
        'Specimen/60c67a06-ea2d-4d24-9249-418dc77a16a9': {'identifier': 'specimen_1234_labA',
                                                          'resourceType': 'Specimen',
                                                          'id': '60c67a06-ea2d-4d24-9249-418dc77a16a9',
                                                          'collection': 'Breast', 'processing': 'Double-Spun'},
        'Observation/cec32723-9ede-5f24-ba63-63cb8c6a02cf': {'identifier': 'patientX_1234-9ae7e542-767f-4b03-a854-7ceed17152cb-sequencer', 'resourceType': 'Observation',
                                                             'id': 'cec32723-9ede-5f24-ba63-63cb8c6a02cf',
                                                             'status': 'final', 'category': 'Laboratory',
                                                             'code': 'Gen3 Sequencing Metadata',
                                                             'sequencer': 'Illumina Seq 1000',
                                                             'index': '100bp Single index', 'type': 'Exome',
                                                             'project_id': 'labA_projectXYZ', 'read_length': '100',
                                                             'instrument_run_id': '234_ABC_1_8899',
                                                             'capture_bait_set': 'Human Exom 2X',
                                                             'end_type': 'Paired-End', 'capture': 'emitter XT',
                                                             'sequencing_site': 'AdvancedGeneExom',
                                                             'construction': 'library_construction'},
        'Observation/4e3c6b59-b1fd-5c26-a611-da4cde9fd061': {'identifier': 'patientX_1234-specimen_1234_labA-sample_type',
                                                             'resourceType': 'Observation',
                                                             'id': '4e3c6b59-b1fd-5c26-a611-da4cde9fd061',
                                                             'status': 'final', 'category': 'Laboratory',
                                                             'code': 'labA specimen metadata',
                                                             'sample_type': 'Primary Solid Tumor',
                                                             'library_id': '12345', 'tissue_type': 'Tumor',
                                                             'treatments': 'Trastuzumab',
                                                             'allocated_for_site': 'TEST Clinical Research',
                                                             'indexed_collection_date': '365',
                                                             'biopsy_specimens': 'specimenA, specimenB, specimenC',
                                                             'biopsy_procedure_type': 'Biopsy - Core',
                                                             'biopsy_anatomical_location': 'top axillary lymph node',
                                                             'percent_tumor': '30'},
        'Observation/21f3411d-89a4-4bcc-9ce7-b76edb1c745f': {'identifier': 'patientX_1234-9ae7e542-767f-4b03-a854-7ceed17152cb-Gene', 'resourceType': 'Observation',
                                                             'id': '21f3411d-89a4-4bcc-9ce7-b76edb1c745f',
                                                             'status': 'final', 'category': 'Laboratory',
                                                             'code': 'Genomic structural variant copy number',
                                                             'Gene': 'TP53', 'Chromosome': 'chr17',
                                                             'result': 'gain of function (GOF)'},
        'ResearchStudy/7dacd4d0-3c8e-470b-bf61-103891627d45': {'identifier': 'labA', 'resourceType': 'ResearchStudy',
                                                               'id': '7dacd4d0-3c8e-470b-bf61-103891627d45',
                                                               'name': 'LabA', 'status': 'active',
                                                               'description': 'LabA Clinical Trial Study: FHIR Schema Chorot Integration'},
        'ResearchSubject/2fc448d6-a23b-4b94-974b-c66110164851': {'identifier': 'subjectX_1234',
                                                                 'resourceType': 'ResearchSubject',
                                                                 'id': '2fc448d6-a23b-4b94-974b-c66110164851',
                                                                 'status': 'active'},
        'Organization/89c8dc4c-2d9c-48c7-8862-241a49a78f14': {'identifier': 'LabA_ORGANIZATION',
                                                              'resourceType': 'Organization',
                                                              'id': '89c8dc4c-2d9c-48c7-8862-241a49a78f14',
                                                              'type': 'Educational Institute'},
        'Patient/bc4e1aa6-cb52-40e9-8f20-594d9c84f920': {'identifier': 'patientX_1234', 'resourceType': 'Patient',
                                                         'id': 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'active': True}}


@pytest.fixture()
def smmart_local_db():
    """Load a local db with smmart data fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # print(f"Temporary directory created at: {temp_dir}")
        db = LocalFHIRDatabase(db_name=os.path.join(temp_dir, 'local.db'))
        fixture_path = pathlib.Path('tests/fixtures/fhir-compbio-examples/META')
        assert fixture_path.exists(), f"Fixture path {fixture_path.absolute()} does not exist."
        for file in fixture_path.glob('*.ndjson'):
            # print(f"Loading {file}")
            for resource in read_ndjson_file(str(file)):
                db.insert_data_from_dict(resource)
        yield db


@pytest.fixture()
def document_reference_dataframe():
    # TODO - is this the expected output?
    return {'identifier': '9ae7e542-767f-4b03-a854-7ceed17152cb',
     'resourceType': 'DocumentReference',
     'id': '9ae7e542-767f-4b03-a854-7ceed17152cb',
     'status': 'current', 'docStatus': 'final',
     'date': '2024-08-21T10:53:00+00:00',
     'specimen_identifier': 'specimen_1234_labA',
     'specimen_id': '60c67a06-ea2d-4d24-9249-418dc77a16a9',
     'specimen_collection': 'Breast',
     'specimen_processing': 'Double-Spun',
     'sequencer': 'Illumina Seq 1000',
     'index': '100bp Single index', 'type': 'Exome',
     'project_id': 'labA_projectXYZ',
     'read_length': '100',
     'instrument_run_id': '234_ABC_1_8899',
     'capture_bait_set': 'Human Exom 2X',
     'end_type': 'Paired-End', 'capture': 'emitter XT',
     'sequencing_site': 'AdvancedGeneExom',
     'construction': 'library_construction',
     'Gene': 'TP53', 'Chromosome': 'chr17',
     'result': 'gain of function (GOF)'}


@pytest.fixture()
def expected_keys(simplified_smmart_resources):
    return sorted(list(simplified_smmart_resources.keys()))


@pytest.fixture()
def smmart_resources(smmart_local_db):
    cursor = smmart_local_db.connection.cursor()
    cursor.execute("SELECT * FROM resources")
    resources = cursor.fetchall()
    _resources = []
    for resource in resources:
        key, resource_type, resource = resource
        resource = json.loads(resource)
        _resources.append(resource)
    return _resources


def test_smmart_db(smmart_local_db, expected_keys):
    """Simple test to verify db load."""
    assert smmart_local_db
    cursor = smmart_local_db.connection.cursor()
    cursor.execute("SELECT * FROM resources")
    resources = cursor.fetchall()
    actual_keys = []
    for resource in resources:
        key, resource_type, resource = resource
        _ = json.loads(resource)
        actual_keys.append(key)
    actual_keys = sorted(actual_keys)
    print(actual_keys)
    assert actual_keys == expected_keys


def test_simplified(smmart_resources, simplified_smmart_resources):
    """Simple test to verify resources are simplified (no joins)."""
    actual = {}
    for resource in smmart_resources:
        assert isinstance(resource, dict), f"Expected dict, got {type(resource)} {resource}"
        simplified = SimplifiedResource.build(resource=resource).simplified
        actual[f"{simplified['resourceType']}/{simplified['id']}"] = simplified
    print(actual)
    assert actual == simplified_smmart_resources


def test_smmart_document_reference(smmart_local_db, document_reference_dataframe):
    """Test the dataframer using a local database with a SMMART bundle, this test ensures document reference and all its Observations."""
    # TODO - once we are happy w/ this, move it to gen3_tracker.meta.dataframer
    cursor = smmart_local_db.connection.cursor()

    # get the document reference
    document_reference_key = 'DocumentReference/9ae7e542-767f-4b03-a854-7ceed17152cb'
    cursor.execute("SELECT * FROM resources WHERE key = ?", (document_reference_key,))
    _ = cursor.fetchone()
    assert _, f"{document_reference_key} not found"
    key, resource_type, resource = _
    resource = json.loads(resource)

    # simplify it
    simplified = SimplifiedResource.build(resource=resource).simplified
    document_reference = resource

    # get its subject, simplify it and add it to the simplified document reference
    subject_key = document_reference['subject']['reference']
    cursor.execute("SELECT * FROM resources WHERE key = ?", (subject_key,))
    _ = cursor.fetchone()
    assert _, f"{subject_key} not found"
    key, resource_type, resource = _
    resource = json.loads(resource)
    simplified_subject = SimplifiedResource.build(resource=resource).simplified
    prefix = simplified_subject['resourceType'].lower()
    for k, v in simplified_subject.items():
        if k == 'resourceType':
            continue
        simplified[f"{prefix}_{k}"] = v

    # get its focus, simplify it and add it to the simplified document reference
    if 'focus' in document_reference and len(document_reference['focus']) > 0:
        focus_key = document_reference['focus'][0]['reference']
        cursor.execute("SELECT * FROM resources WHERE key = ?", (focus_key,))
        _ = cursor.fetchone()
        assert _, f"{focus_key} not found"
        key, resource_type, resource = _
        resource = json.loads(resource)
        simplified_focus = SimplifiedResource.build(resource=resource).simplified
        prefix = simplified_focus['resourceType'].lower()
        for k, v in simplified_focus.items():
            if k == 'resourceType':
                continue
            simplified[f"{prefix}_{k}"] = v

    # get all Observations that are focused on the document reference, simplify them and add them to the simplified document reference
    cursor.execute("SELECT * FROM resources WHERE resource_type = ?", ('Observation',))
    observations = cursor.fetchall()
    for observation in observations:
        key, resource_type, resource = observation
        resource = json.loads(resource)
        foci = resource.get('focus', [])
        references = [focus['reference'] for focus in foci]
        if document_reference_key not in references:
            continue

        simplified_observation = SimplifiedResource.build(resource=resource).simplified
        if 'value' in simplified_observation:
            # no component, simple observation
            code = inflection.underscore(inflection.parameterize(simplified_observation['code']))
            value = simplified_observation['value']
            # TODO - should we prefix the component keys? e.g. observation_component_value
            simplified[code] = value
        else:
            # component
            for k, v in simplified_observation.items():
                if k in ['resourceType', 'id', 'category', 'code', 'status']:
                    continue
                # TODO - should we prefix the component keys? e.g. observation_component_value
                simplified[k] = v

    print(simplified)
    assert simplified == document_reference_dataframe
