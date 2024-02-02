import pytest
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.encounter import Encounter
from fhir.resources.fhirtypes import Code

from gen3_util.meta import parse_obj


@pytest.mark.skip("TODO - getting Missing `display` resource_type='Coding' FHIR validation error when RUN as part of a test suite?")
def test_coding():
    """Do we need display?"""

    # with display
    _ = {
        "resourceType": "Coding",
        "code": "code",
        "system": "http://system",
        "display": "display"
    }
    parse_result = parse_obj(_, validate=True)
    assert parse_result.exception is None, parse_result.exception
    assert isinstance(parse_result.resource, Coding), parse_result.resource.cls
    assert parse_result.resource.display == "display", parse_result.resource.display

    # no display
    _ = {
        "resourceType": "Coding",
        "code": "code",
        "system": "http://system",
    }
    parse_result = parse_obj(_, validate=True)
    assert parse_result.exception is None, parse_result.exception
    assert isinstance(parse_result.resource, Coding), parse_result.resource.cls
    assert parse_result.resource.display is None, parse_result.resource.display


@pytest.mark.skip("TODO - getting Missing `display` resource_type='Coding' FHIR validation error when RUN as part of a test suite?")
def test_encounter():
    """Does embedded coding need display?"""
    _ = {
        "resourceType": "Encounter",
        "id": "dc29c301-5af9-5f8c-5a8c-88c58257bbbf",
        "meta": {
            "profile": ["http://hl7.org/fhir/us/core/StructureDefinition/us-core-encounter"]
        },
        "identifier": [
            {
                "use": "official",
                "system": "https://github.com/synthetichealth/synthea",
                "value": "dc29c301-5af9-5f8c-5a8c-88c58257bbbf",
            }
        ],
        "status": "finished",
        "class": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                        "code": "AMB",
                    }
                ]
            }
        ],
        "type": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "162673000",
                        "display": "General examination of patient (procedure)",
                    }
                ],
                "text": "General examination of patient (procedure)",
            }
        ],
        "subject": {
            "reference": "Patient/09b0050e-6705-9aca-6738-2b753d0dc7f4",
            "display": "Mr. Ty725 Padberg411",
        },
        "participant": [
            {
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "PPRF",
                                "display": "primary performer",
                            }
                        ],
                        "text": "primary performer",
                    }
                ],
                "period": {
                    "start": "1992-03-22T06:43:51-08:00",
                    "end": "1992-03-22T06:58:51-08:00",
                },
                "actor": {
                    "reference": "Practitioner?identifier=http://hl7.org/fhir/sid/us-npi%7C9999658195",
                    "display": "Dr. Ayana300 Cassin499",
                },
            }
        ],
        "location": [
            {
                "location": {
                    "reference": "Location?identifier=https://github.com/synthetichealth/synthea%7C39f509d9-dedc-33a7-9a1f-4163f263d530",
                    "display": "MERRIMACK VALLEY PULMONARY ASSOCIATES, P.C.",
                }
            }
        ],
        "serviceProvider": {
            "reference": "Organization?identifier=https://github.com/synthetichealth/synthea%7C8fc48197-92c4-3868-ad3e-d177a8e0fc23",
            "display": "MERRIMACK VALLEY PULMONARY ASSOCIATES, P.C.",
        },
        "actualPeriod": {
            "start": "1992-03-22T06:43:51-08:00",
            "end": "1992-03-22T06:58:51-08:00",
        },
    }

    parse_result = parse_obj(_, validate=True)
    assert parse_result.exception is None, parse_result.exception
    assert isinstance(parse_result.resource, Encounter), type(parse_result.resource)
    encounter: Encounter = parse_result.resource
    assert encounter.status == "finished", encounter.status
    codeable_concept: CodeableConcept = encounter.class_fhir[0]
    code: Code = codeable_concept.coding[0]
    assert code.code == "AMB"
    assert code.display is None
