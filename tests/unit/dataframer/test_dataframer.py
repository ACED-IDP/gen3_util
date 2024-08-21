import json
import os
import tempfile

import inflection
import pytest

from gen3_tracker.meta.dataframer import LocalFHIRDatabase
from tests.unit.dataframer.dataframer import SimplifiedResource


@pytest.fixture()
def smmart_bundle():
    return {
        "resourceType": "Bundle",
        "id": "example-bundle",
        "type": "collection",
        "entry": [
            {
                "fullUrl": "Patient/patient",
                "resource": {
                    "resourceType": "Patient",
                    "id": "patient",
                    "name": [
                        {
                            "use": "official",
                            "family": "Smith",
                            "given": [
                                "John"
                            ]
                        }
                    ],
                    "gender": "male",
                    "birthDate": "1980-05-15"
                }
            },
            {
                "fullUrl": "Specimen/specimen",
                "resource": {
                    "resourceType": "Specimen",
                    "id": "specimen",
                    "type": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "119331000119101",
                                "display": "Blood specimen"
                            }
                        ]
                    },
                    "subject": {
                        "reference": "Patient/patient"
                    },
                    "collection": {
                        "collectedDateTime": "2024-08-14T10:00:00Z"
                    }
                }
            },
            {
                "fullUrl": "DocumentReference/document-reference",
                "resource": {
                    "resourceType": "DocumentReference",
                    "id": "document-reference",
                    "status": "current",
                    "docStatus": "preliminary",
                    "type": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "11506-3",
                                "display": "Laboratory report"
                            }
                        ]
                    },
                    "subject": {
                        "reference": "Patient/patient"
                    },
                    "focus": [
                        {
                            "reference": "Specimen/specimen"
                        }
                    ],
                    "date": "2024-08-14",
                    "description": "Laboratory report for specimen analysis"
                }
            },
            {
                "fullUrl": "Observation/observation-1",
                "resource": {
                    "resourceType": "Observation",
                    "id": "observation-1",
                    "status": "final",
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "8480-6",
                                "display": "Body temperature"
                            }
                        ]
                    },
                    "subject": {
                        "reference": "Patient/patient"
                    },
                    "focus": [
                        {
                            "reference": "DocumentReference/document-reference"
                        }
                    ],
                    "valueQuantity": {
                        "value": 36.6,
                        "unit": "Cel",
                        "system": "http://unitsofmeasure.org",
                        "code": "Cel"
                    },
                    "effectiveDateTime": "2024-08-14T10:00:00Z"
                }
            },
            {
                "fullUrl": "Observation/observation-2",
                "resource": {
                    "resourceType": "Observation",
                    "id": "observation-2",
                    "status": "final",
                    "code": {
                        "coding": [
                            {
                                "system": "http://loinc.org",
                                "code": "2093-3",
                                "display": "Heart rate"
                            }
                        ]
                    },
                    "subject": {
                        "reference": "Patient/patient"
                    },
                    "focus": [
                        {
                            "reference": "DocumentReference/document-reference"
                        }
                    ],
                    "valueQuantity": {
                        "value": 72,
                        "unit": "beats/minute",
                        "system": "http://unitsofmeasure.org",
                        "code": "beats/minute"
                    },
                    "effectiveDateTime": "2024-08-14T10:00:00Z"
                }
            }
        ]
    }


@pytest.fixture()
def simplified_smmart_bundle():
    return {'Patient/patient': {'identifier': None, 'resourceType': 'Patient', 'id': 'patient', 'gender': 'male',
                                'birthDate': '1980-05-15'},
            'Specimen/specimen': {'identifier': None, 'resourceType': 'Specimen', 'id': 'specimen',
                                  'type': 'Blood specimen'},
            'DocumentReference/document-reference': {'identifier': None, 'resourceType': 'DocumentReference',
                                                     'id': 'document-reference', 'status': 'current',
                                                     'docStatus': 'preliminary', 'date': '2024-08-14',
                                                     'description': 'Laboratory report for specimen analysis',
                                                     'type': 'Laboratory report'},
            'Observation/observation-1': {'identifier': None, 'resourceType': 'Observation', 'id': 'observation-1',
                                          'status': 'final', 'effectiveDateTime': '2024-08-14T10:00:00Z',
                                          'code': 'Body temperature', 'value': '36.6 Cel'},
            'Observation/observation-2': {'identifier': None, 'resourceType': 'Observation', 'id': 'observation-2',
                                          'status': 'final', 'effectiveDateTime': '2024-08-14T10:00:00Z',
                                          'code': 'Heart rate', 'value': '72 beats/minute'}}


@pytest.fixture()
def smmart_local_db(smmart_bundle):
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Temporary directory created at: {temp_dir}")
        db = LocalFHIRDatabase(db_name=os.path.join(temp_dir, 'local.db'))
        for entry in smmart_bundle['entry']:
            db.insert_data_from_dict(entry['resource'])
        yield db


@pytest.fixture()
def document_reference_dataframe():
    return {
        # from document reference
        'identifier': None, 'resourceType': 'DocumentReference', 'id': 'document-reference', 'status': 'current',
        'docStatus': 'preliminary', 'date': '2024-08-14', 'description': 'Laboratory report for specimen analysis',
        'type': 'Laboratory report',
        # from subject
        'patient_identifier': None, 'patient_id': 'patient', 'patient_gender': 'male',
        'patient_birthDate': '1980-05-15',
        # from focus
        'specimen_identifier': None, 'specimen_id': 'specimen',
        'specimen_type': 'Blood specimen',
        # from observations
        'body_temperature': '36.6 Cel', 'heart_rate': '72 beats/minute'}


def test_dataframe_smmart(smmart_bundle, simplified_smmart_bundle):
    """Test the dataframer using a SMMART bundle, this test just simplifies each object individually."""
    for entry in smmart_bundle['entry']:
        resource = entry['resource']
        simplified = SimplifiedResource.build(resource=resource).simplified
        assert simplified == simplified_smmart_bundle[f"{resource['resourceType']}/{resource['id']}"]


def test_smmart_document_reference(smmart_local_db, simplified_smmart_bundle, document_reference_dataframe):
    """Test the dataframer using a local database with a SMMART bundle, this test ensures document reference and all its Observations."""
    cursor = smmart_local_db.connection.cursor()

    # get the document reference
    document_reference_key = 'DocumentReference/document-reference'
    cursor.execute("SELECT * FROM resources WHERE key = ?", (document_reference_key,))
    _ = cursor.fetchone()
    assert _, f"{document_reference_key} not found"
    key, resource_type, resource = _
    resource = json.loads(resource)

    # simplify it
    simplified = SimplifiedResource.build(resource=resource).simplified
    assert simplified == simplified_smmart_bundle[document_reference_key]
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
        code = inflection.underscore(inflection.parameterize(simplified_observation['code']))
        value = simplified_observation['value']
        simplified[code] = value

    print(simplified)
    assert simplified == document_reference_dataframe
