##### META folder test-data:

```
>>>> {'summary': {'DocumentReference': 1, 'Specimen': 1, 'Observation': 8, 'ResearchStudy': 1, 'Condition': 1, 'ResearchSubject': 1, 'Encounter': 1, 'Organization': 1, 'Patient': 1}}
```

Three Observations with user-defined metadata component. 
1. Focus - reference -> Specimen
2. Focus - reference -> DocumentReference 
   1. The first Observation contains metadata on the file's sequencing metadata.
   2. The second Observation includes a simple summary of a CNV analysis result computed from this file.

Five Observations to define [AJCC](https://www.facs.org/media/j30havyf/ajcc_7thed_cancer_staging_manual.pdf) Cancer Stage with TNM and Grade classifications.
Current Condition has a ConditionStage that holds definitions for all stage classifications and references to its associated observation.
Encounter entity is present to demo the event where patients' diagnosis was made and their specimen(s) were collected for the pathological stage classification. 