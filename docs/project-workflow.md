# Project Workflow

## Dependencies

* Meta data management: `~/.gen3/credentials.json` [identity file](https://gen3.org/resources/user/using-api/#credentials-to-send-api-requests) from the [portal](https://aced-training.compbio.ohsu.edu/identity)
* Upload and download: A configured [gen3-client](https://gen3.org/resources/user/gen3-client/#1-installation-instructions) for file uploads and downloads

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
find tests/fixtures/add_files_to_study -type f  | xargs -P 9 -I PATH gen3_util files manifest put PATH

```

## Check the file names in the upload manifest

```text
gen3_util files manifest ls | grep file_name
#  file_name: ...
```

## Upload and index the files

```text
gen3_util files manifest upload --profile $GEN3_CLIENT_PROFILE

```

## Create basic, minimal metadata for the project

```text
gen3_util meta create indexd /tmp/$PROJECT_ID
```

### Optional: edit or validate the metadata

The `meta create` command will create minimal FHIR resources for the files and identifiers.  You may wish to enhance the meta data by populating additional attributes.
You may also provide your own FHIR resource files.   Refer to https://aced-training.compbio.ohsu.edu/DD for additional information.

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

## Add file to the project, we assign a patient identifier to the file

> Note: the identifiers used are arbitrary, they are not validated.
> * a unique alphanumeric identifier for that record across the whole project and is specified by the data submitter
> * **No PHI values should be used.**
> For more see: https://build.fhir.org/datatypes.html#Identifier
>
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
gen3_util files manifest upload --profile $GEN3_CLIENT_PROFILE

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

#### Check that your file and meta data have been successfully uploaded

<img width="1485" alt="image" src="https://github.com/ACED-IDP/gen3_util/assets/47808/d4d8c6bf-bb9a-49cf-affc-34daf78ce92c">


## Use case: analyze data, generate metadata and publish to the portal

* Assuming project exists, files have been uploaded and indexed, and metadata has been created and published.
* Query the system, produce file manifest. An example using the windmill ui:

![image](https://github.com/ACED-IDP/gen3_util/assets/47808/3aa0af6d-0112-4d6b-bb60-3749ef6b482f)

### Download the manifest using `gen3-client`:

#### Environment setup


```commandline
EXPORT PROJECT_ID=test-Alcoholism

# see https://gen3.org/resources/user/gen3-client/#2-configure-a-profile-with-credentials
EXPORT GEN3_CLIENT_PROFILE=<profile>
```
#### Download files using manifest

```commandline
gen3-client download-multiple --manifest  manifest-alcoholism.json  --profile $GEN3_CLIENT_PROFILE --filename-format original  --download-path /tmp/alcoholism --numparallel 9  --no-prompt
# ...
# 2023/10/02 12:57:36 14 files downloaded.

```

#### Process the downloaded files, using [pydicom](https://pydicom.github.io/) to extract file info

```commandline
find /tmp/alcoholism   -name '*.dcm' | xargs -n 1  pydicom show > analysis/dicom-info.txt

```

#### Add the resulting analysis file to the manifest

```commandline
gen3_util files manifest put analysis/dicom-info.txt
```

#### Check manifest

```commandline
gen3_util files manifest ls
# - object_id: ...
#  file_name: ...
```

#### Upload and index the file

```commandline
gen3_util files manifest upload --profile $GEN3_CLIENT_PROFILE
```

#### Create metadata for the analysis files. In this example, create minimal metadata using information uploaded into indexd.

```commandline
gen3_util meta create indexd meta/
```

#### Optionally edit or validate the metadata

```commandline
gen3_util meta validate meta
```

#### Publish the metadata to the portal

```commandline
gen3_util meta publish meta/
```

#### Check that your file and meta data have been successfully uploaded
![image](https://github.com/ACED-IDP/gen3_util/assets/47808/c705eb32-f636-42e2-992e-e076f1b28cb8)
