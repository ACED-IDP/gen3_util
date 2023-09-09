# Project Workflow

## Create a project in authorization system

```text
# request a project create a project
gen3_util projects add resource test-myproject

# user with appropriate authority signs requests for the project
gen3_util access sign --project_id test-myproject

```



## add files to the project

```text
# find all files under tests/fixtures/add_files_to_study
find tests/fixtures/add_files_to_study -type f  | xargs  -I PATH gen3_util files put --project_id  test-myproject PATH bucket://aced-development-ohsu-data-bucket

```

## Create project metadata

```text
# create basic, minimal metadata for the project
gen3_util meta import indexd --project_id test-myproject /tmp/test-myproject
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
gen3_util meta publish  /tmp/test-myproject  bucket://aced-development-ohsu-data-bucket --project_id test-myproject

```


## View in portal
<img alt="image" src="https://github.com/ACED-IDP/data_model/assets/47808/133ef835-63d6-473e-80ad-9c4e0de62651">
