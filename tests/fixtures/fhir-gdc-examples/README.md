##### META folder test-data:

```
{'summary': {'DocumentReference': 1, 'Specimen': 3, 'Patient': 1}}
```

Simplified version of GDC data demonstrating how a DocumentReference may have reference links to a **Patient** or **Specimen**: 
1. basedOn - reference -> list of Specimen(s)
2. subject - reference -> Patient

In some rare GDA use-cases, subject may reference a [**Group**](https://build.fhir.org/group.html) of patients. Here we only focus one patient for simplicity. 

