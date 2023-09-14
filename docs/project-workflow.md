# Project Workflow

## Create a project in authorization system

```text
# request a project create a project
gen3_util projects add resource test-myproject

# user with appropriate authority signs requests for the project
gen3_util access sign --project_id test-myproject

```

# Use case: upload files and associate them with a study


## add files to the project

```text
# find all files under tests/fixtures/add_files_to_study
find tests/fixtures/add_files_to_study -type f  | xargs  -I PATH gen3_util files put --project_id  test-myproject PATH

```

## Create project metadata

```text
# create basic, minimal metadata for the project
gen3_util meta create indexd --project_id test-myproject /tmp/test-myproject
```

### Optional: edit the metadata
```text
# files created from the previous step
ls -1 /tmp/test-myproject
DocumentReference.ndjson
ResearchStudy.ndjson
```


## Publish the project metadata to the portal

```text
# copy the metadata to the bucket and publish the metadata to the portal
gen3_util meta publish  /tmp/test-myproject --project_id test-myproject

```


## View in portal
<img alt="image" src="https://github.com/ACED-IDP/data_model/assets/47808/133ef835-63d6-473e-80ad-9c4e0de62651">


# Use case: upload files and associate them with a patient, specimen or observation

## Add file to the project, note we assign a patient identifier to the file

```commandline
gen3_util files put --project_id  test-myproject \
    --patient_id patient-1 \
    tests/fixtures/add_files_to_study/file-1.txt
```

## Add file to the project, note we assign a patient identifier and specimen identifier to the file

```commandline
gen3_util files put --project_id  test-myproject \
    --patient_id patient-1 \
    --specimen_id specimen-1 \
    tests/fixtures/add_files_to_study/file-2.csv
```

## Add file to the project, note we assign a patient identifier and observation identifier to the file

```commandline
gen3_util files put --project_id  test-myproject \
    --patient_id patient-1 \
    --observation_id observation-1 \
    tests/fixtures/add_files_to_study/sub-dir/file-3.pdf
```

## Add file to the project, note we assign a patient identifier, specimen identifier and a task identifier to the file

```commandline

gen3_util files put --project_id  test-myproject \
    --patient_id patient-1 \
    --observation_id observation-2 \
    --task_id task-1 \
    tests/fixtures/add_files_to_study/sub-dir/file-4.tsv

```

## Create project metadata

```text
# create basic, minimal metadata for the project
gen3_util meta create indexd --project_id test-myproject /tmp/test-myproject
```

### Optional: edit the metadata

```commandline
ls -1 /tmp/test-myproject/
DocumentReference.ndjson
Observation.ndjson
Patient.ndjson
ResearchStudy.ndjson
ResearchSubject.ndjson
Specimen.ndjson
Task.ndjson
```


## Publish the project metadata to the portal

```text
# copy the metadata to the bucket and publish the metadata to the portal
gen3_util meta publish  /tmp/test-myproject --project_id test-myproject

```


## View in portal

<img width="1485" alt="image" src="https://github.com/ACED-IDP/gen3_util/assets/47808/d4d8c6bf-bb9a-49cf-affc-34daf78ce92c">
