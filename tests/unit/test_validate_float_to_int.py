import pytest
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient
from pydantic import ValidationError


def test_validate_observation():
    """Test validate observation."""
    false = False
    observation_dict = {
        "resourceType": "Observation",
        "id": "9d11e26b-0307-5573-aee8-d145bdc259f3",
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "laboratory",
                        "display": "Laboratory"
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "81247-9",
                    "display": "Master HL7 genetic variant reporting panel"
                }
            ]
        },
        "subject": {
            "reference": "Patient/16244c6a-028a-5d8b-ac80-22e7b870544b"
        },
        "specimen": {
            "reference": "Specimen/f7f2ceb6-53f3-561a-960d-0c47700c14a2"
        },
        "focus": [
            {
                "reference": "Specimen/f7f2ceb6-53f3-561a-960d-0c47700c14a2"
            }
        ],
        "effectiveDateTime": "2024-06-03T08:00:00+00:00",
        "valueString": "Sequencing parameters",
        "component": [
            {
                "code": {
                    "coding": [
                        {
                            "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                            "code": "weight",
                            "display": "weight"
                        }
                    ],
                    "text": "weight"
                },
                "valueInteger": 32.9
            },
            {
                "code": {
                    "coding": [
                        {
                            "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                            "code": "is_ffpe",
                            "display": "is_ffpe"
                        }
                    ],
                    "text": "is_ffpe"
                },
                "valueBoolean": false
            },
            {
                "code": {
                    "coding": [
                        {
                            "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                            "code": "sample_type",
                            "display": "sample_type"
                        }
                    ],
                    "text": "sample_type"
                },
                "valueString": "Solid Tissue Normal"
            },
            {
                "code": {
                    "coding": [
                        {
                            "system": "https://cadsr.cancer.gov/sample_laboratory_observation",
                            "code": "updated_datetime",
                            "display": "updated_datetime"
                        }
                    ],
                    "text": "updated_datetime"
                },
                "valueDateTime": "2018-09-06T17:41:51.247648-05:00"
            }
        ]
    }
    observation_dict['component'][0]['valueInteger'] = 32.0
    observation = Observation.model_validate(observation_dict)

    assert observation, "Should have accepted valueInteger: 32.0"

    observation_dict['component'][0]['valueInteger'] = 32.9

    with pytest.raises(ValidationError):
        Observation.model_validate(observation_dict)


def test_patient():
    with pytest.raises(ValidationError):
        patient_dict = {"multipleBirthInteger": 32.9}
        patient = Patient.model_validate(patient_dict)
        assert patient.multipleBirthInteger == 32.9, "Should not have accepted multipleBirthInteger: 32.9"

    patient_dict = {"multipleBirthInteger": 32.0}
    Patient.model_validate(patient_dict)

    patient_dict = {"multipleBirthInteger": 32}
    Patient.model_validate(patient_dict)
