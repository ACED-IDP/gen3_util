# Project Workflow

## Usage:

`gen3_util projects [OPTIONS] COMMAND [ARGS]...`

  The following `--parameters` and environmental variables are synonymous:

    * OBSERVATION_ID
    * PATIENT_ID
    * PROJECT_ID
    * SPECIMEN_ID
    * TASK_ID

  You may set them as environmental variables or pass them as parameters to the command line.

    * `--observation_id=observation-1` or `export OBSERVATION_ID=observation-1`

  The following parameters may be used to control system wide behavior:

    * GEN3_UTIL_CONFIG: Path to config file. [default: None]
    * GEN3_UTIL_FORMAT: Output format. [default yaml]
    * GEN3_UTIL_STATE_DIR: Path for logs and state information.  [default: ~/.gen3/gen3_util]
    * GEN3_API_KEY: location of credentials.json file. [default: ~/.gen3/credentials.json]


## Create a project in authorization system

```text
export PROJECT_ID=test-myproject
# request a project create a project
gen3_util projects new

# user with appropriate authority signs requests for the project
# optionally, use the `--username nancy@example.com` to limit the approvals to a specific user
gen3_util access sign

```

# Use case: upload files and associate them with a study


## add files to the manifest

```text
# find all files under tests/fixtures/add_files_to_study
find tests/fixtures/add_files_to_study -type f  | xargs  -I PATH gen3_util files manifest put PATH

```

```text
# list the file names in the upload manifest
$ gen3_util files manifest ls | grep file_name
  file_name: tests/fixtures/dir_to_study/file-1.txt
  file_name: tests/fixtures/add_files_to_study/sub-dir/file-4.tsv
  file_name: tests/fixtures/add_files_to_study/sub-dir/file-3.pdf
  file_name: tests/fixtures/add_files_to_study/sub-dir/file-5
  file_name: tests/fixtures/add_files_to_study/file-1.txt
  file_name: tests/fixtures/add_files_to_study/README.md
  file_name: tests/fixtures/add_files_to_study/file-2.csv

```

## upload and index the files

```text
gen3_util files manifest upload

```

## Create project metadata

```text
# create basic, minimal metadata for the project
gen3_util meta create indexd /tmp/$PROJECT_ID
```

### Optional: edit the metadata
```text
# files created from the previous step
ls -1 /tmp/$PROJECT_ID
DocumentReference.ndjson
ResearchStudy.ndjson
```


## Publish the project metadata to the portal

```text
# copy the metadata to the bucket and publish the metadata to the portal
gen3_util meta publish  /tmp/$PROJECT_ID

```


## View in portal
<img alt="image" src="https://github.com/ACED-IDP/data_model/assets/47808/133ef835-63d6-473e-80ad-9c4e0de62651">


# Use case: upload files and associate them with a patient, specimen or observation

## Create a project in authorization system

> see above

## Add file to the project, note we assign a patient identifier to the file

```commandline

export PROJECT_ID=test-myproject

gen3_util files manifest put \
    --patient_id patient-1 \
    tests/fixtures/add_files_to_study/file-1.txt
```

## Add file to the project, note we assign a patient identifier and specimen identifier to the file

```commandline
gen3_util files manifest put \
    --patient_id patient-1 \
    --specimen_id specimen-1 \
    tests/fixtures/add_files_to_study/file-2.csv
```

## Add file to the project, note we assign a patient identifier and observation identifier to the file

```commandline
gen3_util files manifest put \
    --patient_id patient-1 \
    --observation_id observation-1 \
    tests/fixtures/add_files_to_study/sub-dir/file-3.pdf
```

## Add file to the project, note we assign a patient identifier, specimen identifier and a task identifier to the file

```commandline

gen3_util files manifest put \
    --patient_id patient-1 \
    --observation_id observation-2 \
    --task_id task-1 \
    tests/fixtures/add_files_to_study/sub-dir/file-4.tsv

```

## upload and index the files

```text
gen3_util files manifest upload

```


## Create project metadata

```text
# create basic, minimal metadata for the project
gen3_util meta create indexd /tmp/$PROJECT_ID
```

### Optional: edit the metadata

```commandline
ls -1 /tmp/$PROJECT_ID
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
gen3_util meta publish  /tmp/$PROJECT_ID

```


## View in portal

<img width="1485" alt="image" src="https://github.com/ACED-IDP/gen3_util/assets/47808/d4d8c6bf-bb9a-49cf-affc-34daf78ce92c">
