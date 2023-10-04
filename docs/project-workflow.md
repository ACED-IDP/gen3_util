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

```text
# list the file names in the upload manifest
gen3_util files manifest ls | grep file_name
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
gen3_util files manifest upload --profile $GEN3_CLIENT_PROFILE

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


## View in portal

<img width="1485" alt="image" src="https://github.com/ACED-IDP/gen3_util/assets/47808/d4d8c6bf-bb9a-49cf-affc-34daf78ce92c">


## Use case: analyze data, generate metadata and publish to the portal

* Assuming project exists, files have been uploaded and indexed, and metadata has been created and published.
* Query the system, produce file manifest. An example using the windmill ui:

![image](https://github.com/ACED-IDP/gen3_util/assets/47808/3aa0af6d-0112-4d6b-bb60-3749ef6b482f)

* Download the manifest using `gen3-client`:

```commandline
export PROJECT_ID=test-Alcoholism

# see https://gen3.org/resources/user/gen3-client/#2-configure-a-profile-with-credentials
EXPORT GEN3_CLIENT_PROFILE=<profile>

$gen3-client download-multiple --manifest  manifest-alcoholism.json  --profile $GEN3_CLIENT_PROFILE --filename-format original  --download-path /tmp/alcoholism --numparallel 9  --no-prompt
2023/10/02 12:56:52 Reading manifest...
 912 B / 912 B [==============================================================================================================] 100.00% 0s
WARNING: flag "rename" was set to false in "original" mode, duplicated files under "/tmp/alcoholism/" will be overwritten
2023/10/02 12:56:52 Total number of objects in manifest: 14
2023/10/02 12:56:52 Preparing file info for each file, please wait...
 14 / 14 [====================================================================================================================] 100.00% 0s
2023/10/02 12:56:53 File info prepared successfully
output/dicom/fec6d99f-1cfd-f397-e740-e3952410ea2a1.2.840.99999999.64484254.723245133887.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/95e41dbd-bcaa-6b59-b142-0d8b5826c2d21.2.840.99999999.88209121.1316426860731.dcm  32.02 MiB / 32.02 MiB [============] 100.00%
output/dicom/0122b006-83c5-b1fa-cb1d-a75934d9aef11.2.840.99999999.98603446.970854118536.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/80f28bab-c204-0a96-ea17-ab2b383fa9c21.2.840.99999999.57504167.766741206412.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/745a318e-857e-da43-e673-8ce73f417c281.2.840.99999999.21568346.1325907305064.dcm  32.02 MiB / 32.02 MiB [============] 100.00%
output/dicom/e791826c-265f-f003-9892-0ab7e34f0ab51.2.840.99999999.52576007.462162575656.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/c7217682-6110-1ca5-1992-a17d18afe1a71.2.840.99999999.23660322.844779366809.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/47ee0f48-5e53-0299-7cfe-5519e4bf88d21.2.840.99999999.75265158.1012731643969.dcm  32.02 MiB / 32.02 MiB [============] 100.00%
output/dicom/54fbe1a2-1edd-d277-81e1-499f8e44cd8c1.2.840.99999999.73424168.804504979864.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/5a676f76-34bb-39a9-87ed-e03e5137b0981.2.840.99999999.67278613.571046200769.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/47a14779-8178-a001-ca12-2d2c96fb9f491.2.840.99999999.61371396.1038496157027.dcm  32.02 MiB / 32.02 MiB [============] 100.00%
output/dicom/e522c2e3-dd23-8195-f693-3f94c32e448e1.2.840.99999999.68592107.776460444442.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/0b73e9c9-18c0-5c0e-c5d5-95d7ca56c0501.2.840.99999999.71548685.939989616743.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
output/dicom/2d5baef5-24fc-58cc-af18-6838f1f87e4d1.2.840.99999999.24864379.980779828877.dcm  32.02 MiB / 32.02 MiB [=============] 100.00%
2023/10/02 12:57:36 14 files downloaded.



```

* Process the downloaded files, using pydicom to extract file info

```commandline
find /tmp/alcoholism   -name '*.dcm' | xargs -n 1  pydicom show > analysis/dicom-info.txt

```

* Add the resulting analysis file to the manifest

```commandline
gen3_util files manifest put analysis/dicom-info.txt
```

* List the manifest

```commandline
gen3_util files manifest ls
- object_id: ...
  file_name: ...
```

* Upload and index the file

```commandline
gen3_util files manifest upload --profile $GEN3_CLIENT_PROFILE
```

* Create metadata for the analysis files. In this example, create minimal metadata using information uploaded into indexd.

```commandline
gen3_util meta create indexd meta/
```

* Optionally edit or validate the metadata

```commandline
gen3_util meta validate meta
```

* Publish the metadata to the portal

```commandline
gen3_util meta publish meta/
```

* View in portal
![image](https://github.com/ACED-IDP/gen3_util/assets/47808/c705eb32-f636-42e2-992e-e076f1b28cb8)
