
import pytest


@pytest.fixture()
def specimen() -> dict:
    return {
        "resourceType": "Specimen",
        "id": "101",
        "text": {
            "status": "generated",
            "div": "TEXT ..."
        },
        "identifier": [{
            "system": "http://ehr.acme.org/identifiers/collections",
            "value": "23234352356"
        }],
        "accessionIdentifier": {
            "system": "http://lab.acme.org/specimens/2011",
            "value": "X352356"
        },
        "status": "available",
        "type": {
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "122555007",
                "display": "Venous blood specimen"
            }]
        },
        "subject": {
            "reference": "Patient/example",
            "display": "Peter Patient"
        },
        "receivedTime": "2011-03-04T07:03:00Z",
        "request": [{
            "reference": "ServiceRequest/example"
        }],
        "collection": {
            "collector": {
                "reference": "Practitioner/example"
            },
            "collectedDateTime": "2011-05-30T06:15:00Z",
            "quantity": {
                "value": 6,
                "unit": "mL"
            },
            "method": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0488",
                    "code": "LNV"
                }]
            },
            "bodySite": {
                "concept": {
                    "coding": [{
                        "system": "http://snomed.info/sct",
                        "code": "49852007",
                        "display": "Structure of median cubital vein (body structure)"
                    }]
                }
            }
        },
        "container": [{
            "device": {
                "reference": "Device/device-example-specimen-container-green-gel-vacutainer"
            },
            "specimenQuantity": {
                "value": 3,
                "unit": "mL"
            }
        }],
        "note": [{
            "text": "Specimen is grossly lipemic"
        }],
        "processing": [{
            "description": "Acidify to pH < 3.0 with 6 N HCl.",
            "method": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0373",
                    "code": "ACID"
                }]
            },
            "additive": [{
                "reference": "Substance/CHEMBL1231821",
                "display": "6 N HCl"
            }],
            "timeDateTime": "2015-08-18T08:10:00Z"
        }],
    }


@pytest.fixture()
def patient() -> dict:
    return {
        "resourceType": "Patient",
        "id": "0e67ef67-bc88-4792-aa70-ccef7db25153",
        "meta": {
            "versionId": "1",
            "lastUpdated": "2023-01-26T14:55:20.239+00:00",
            "source": "#7mKbhP1TZTy3Y1vr",
            "profile": [
                "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"
            ]
        },
        "text": {
            "status": "generated",
            "div": "<div> generated</div>"
        },
        "extension": [
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                "extension": [
                    {
                        "url": "ombCategory",
                        "valueCoding": {
                            "system": "urn:oid:2.16.840.1.113883.6.238",
                            "code": "2106-3",
                            "display": "White"
                        }
                    },
                    {
                        "url": "text",
                        "valueString": "White"
                    }
                ]
            },
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                "extension": [
                    {
                        "url": "ombCategory",
                        "valueCoding": {
                            "system": "urn:oid:2.16.840.1.113883.6.238",
                            "code": "2186-5",
                            "display": "Non Hispanic or Latino"
                        }
                    },
                    {
                        "url": "text",
                        "valueString": "Non Hispanic or Latino"
                    }
                ]
            },
            {
                "url": "http://hl7.org/fhir/StructureDefinition/patient-mothersMaidenName",
                "valueString": "Pandora807 Toy286"
            },
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex",
                "valueCode": "M"
            },
            {
                "url": "http://hl7.org/fhir/StructureDefinition/patient-birthPlace",
                "valueAddress": {
                    "city": "Medford",
                    "state": "Massachusetts",
                    "country": "US"
                }
            },
            {
                "url": "http://synthetichealth.github.io/synthea/disability-adjusted-life-years",
                "valueDecimal": 4.840278904991756
            },
            {
                "url": "http://synthetichealth.github.io/synthea/quality-adjusted-life-years",
                "valueDecimal": 73.15972109500825
            }
        ],
        "identifier": [
            {
                "system": "https://github.com/synthetichealth/synthea",
                "value": "e79d01d9-25bd-ee33-e40b-34cb119ca8e4"
            },
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "MR",
                            "display": "Medical Record Number"
                        }
                    ],
                    "text": "Medical Record Number"
                },
                "system": "http://hospital.smarthealthit.org",
                "value": "e79d01d9-25bd-ee33-e40b-34cb119ca8e4"
            },
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "SS",
                            "display": "Social Security Number"
                        }
                    ],
                    "text": "Social Security Number"
                },
                "system": "http://hl7.org/fhir/sid/us-ssn",
                "value": "999-15-5094"
            },
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "DL",
                            "display": "Driver's License"
                        }
                    ],
                    "text": "Driver's License"
                },
                "system": "urn:oid:2.16.840.1.113883.4.3.25",
                "value": "S99947903"
            },
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "PPN",
                            "display": "Passport Number"
                        }
                    ],
                    "text": "Passport Number"
                },
                "system": "http://standardhealthrecord.org/fhir/StructureDefinition/passportNumber",
                "value": "X47675657X"
            }
        ],
        "name": [
            {
                "use": "official",
                "family": "Walsh511",
                "given": [
                    "Nestor901"
                ],
                "prefix": [
                    "Mr."
                ]
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": "555-905-1070",
                "use": "home"
            }
        ],
        "gender": "male",
        "birthDate": "1917-05-31",
        "deceasedDateTime": "1996-12-19T02:00:49-05:00",
        "address": [
            {
                "extension": [
                    {
                        "url": "http://hl7.org/fhir/StructureDefinition/geolocation",
                        "extension": [
                            {
                                "url": "latitude",
                                "valueDecimal": 42.08152268673541
                            },
                            {
                                "url": "longitude",
                                "valueDecimal": -71.29566704174853
                            }
                        ]
                    }
                ],
                "line": [
                    "640 Bailey Spur"
                ],
                "city": "Norfolk",
                "state": "MA",
                "country": "US"
            }
        ],
        "maritalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
                    "code": "M",
                    "display": "M"
                }
            ],
            "text": "M"
        },
        "multipleBirthBoolean": False,
        "communication": [
            {
                "language": {
                    "coding": [
                        {
                            "system": "urn:ietf:bcp:47",
                            "code": "en-US",
                            "display": "English"
                        }
                    ],
                    "text": "English"
                }
            }
        ]
    }


@pytest.fixture()
def task() -> dict:
    return {
        "resourceType": "Task",
        "id": "a5149327-9dd2-5b6d-8edd-ef50a557a5b1",
        "meta": {
            "versionId": "1",
            "lastUpdated": "2023-01-26T14:21:56.658+00:00",
            "source": "#DmW9sueQ4yuQdyA9"
        },
        "text": {
            "status": "generated",
            "div": "<div xmlns=\"http://www.w3.org/1999/xhtml\">Autogenerated task. Inserted to make data model research friendly.</div>"
        },
        "status": "completed",
        "intent": "order",
        "focus": {
            "reference": "Specimen/c94b2d82-8465-5a29-9636-23ecb8d7f660"
        },
        "for": {
            "reference": "Patient/45c11dad-2b38-4c8e-822e-7abff8a1ee1d"
        },
        "input": [
            {
                "type": {
                    "coding": [
                        {
                            "code": "specimen"
                        }
                    ]
                },
                "valueReference": {
                    "reference": "Specimen/c94b2d82-8465-5a29-9636-23ecb8d7f660"
                }
            }
        ],
        "output": [
            {
                "type": {
                    "coding": [
                        {
                            "code": "DocumentReference"
                        }
                    ]
                },
                "valueReference": {
                    "reference": "DocumentReference/7295ed42-5a87-5ef3-a8f1-08d8e8ab49e7"
                }
            }
        ]
    }


@pytest.fixture()
def observation() -> dict:
    return {
        "resourceType": "Observation", "id": "5d1610df-afd4-4d42-b397-49db9f489a5e",
        "meta": {"versionId": "1", "lastUpdated": "2023-01-26T14:21:56.658+00:00", "source": "#DmW9sueQ4yuQdyA9",
                 "profile": ["http://hl7.org/fhir/StructureDefinition/vitalsigns",
                             "http://hl7.org/fhir/StructureDefinition/heartrate"]}, "status": "final",
        "category": [{"coding": [
            {"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "vital-signs",
             "display": "vital-signs"}]}],
        "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Heart rate"}],
                 "text": "Heart rate"}, "subject": {"reference": "Patient/45c11dad-2b38-4c8e-822e-7abff8a1ee1d"},
        "encounter": {"reference": "Encounter/bd62656a-5e43-46a5-9584-d2402786f7f5"},
        "effectiveDateTime": "2019-01-19T14:40:53-05:00", "issued": "2019-01-19T14:40:53.188-05:00",
        "valueQuantity": {"value": 61, "unit": "/min", "system": "http://unitsofmeasure.org", "code": "/min"}
    }


@pytest.fixture()
def specimen_fields() -> list[str]:
    return [
        'text_div',
        'identifier_0_system', 'identifier_0_value',
        'status',
        'type_coding_0_system', 'type_coding_0_code', 'type_coding_0_display',
        'subject_reference',
        'receivedTime',
        'collection_quantity_value', 'collection_quantity_unit',
        'collection_method_coding_0_system', 'collection_method_coding_0_code',
        'collection_bodySite_concept_coding_0_system', 'collection_bodySite_concept_coding_0_code',
        'collection_bodySite_concept_coding_0_display',
        'processing_0_description', 'processing_0_method_coding_0_system', 'processing_0_method_coding_0_code',
        'processing_0_additive_0_reference', 'processing_0_additive_0_display', 'processing_0_timeDateTime'
    ]
