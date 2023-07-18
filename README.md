
# Gen3 Utilities

Utilities to manage Gen3 schemas, projects and submissions.

## Installation

```

# optionally
$python3 -m venv venv ; source venv/bin/activate

pip install gen3_util

$ gen3_util
msg: Version 0.0.4


```

Note: requires [`magic`](https://github.com/ahupp/python-magic#installation) library. If it is not already installed you will see a warning like this:

```text
Requires libmagic installed on your system to determine mime-types
Error: 'failed to find libmagic.  Check your installation'
For installation instructions see https://github.com/ahupp/python-magic#installation
```

Note: some environments will get a `_ctypes` error.  Please install 3.9.16

## Use

```
$gen3_util --help
Usage: gen3_util [OPTIONS] COMMAND [ARGS]...

  Gen3 Management Utilities

Options:
  --config TEXT              Path to config file. GEN3_UTIL_CONFIG
  --format [yaml|json|text]  Result format. GEN3_UTIL_FORMAT  [default: yaml]
  --cred TEXT                See https://uc-cdis.github.io/gen3-user-
                             doc/appendices/api-gen3/#credentials-to-query-
                             the-api. GEN3_API_KEY
  --state_dir TEXT           Directory for file transfer state
                             GEN3_UTIL_STATE_DIR  [default: ~/.gen3/gen3_util]
  --help                     Show this message and exit.

Commands:
  projects  Manage Gen3 projects.
  buckets   Manage Gen3 buckets.
  meta      Manage meta data.
  files     Manage file buckets.
  access    Manage access requests.
  config    Configure this utility.


```

## Connectivity

* Leverages Gen3Auth  [See](https://uc-cdis.github.io/gen3-user-doc/appendices/api-gen3/#credentials-to-query-the-api.)
* Store the `credentials.json` file in ~/.gen3/credentials.json or specify location with either env[GEN3_API_KEY], or `--cred` parameter

## Use cases

> I need to verify connectivity.

```
$ gen3_util projects ping
msg: OK connected to endpoint https://aced-training.compbio.ohsu.edu
```

> I need to see what projects exist

```
$ gen3_util projects ls

endpoint: https://aced-training.compbio.ohsu.edu
msg: OK
projects:
- /programs
- /programs/aced
- /programs/aced/project
- /programs/aced/project/MCF10A
- /programs/aced/projects
- /programs/aced/projects/Alcoholism
- /programs/aced/projects/Alzheimers
- /programs/aced/projects/Breast_Cancer
- /programs/aced/projects/Colon_Cancer
- /programs/aced/projects/Diabetes
- /programs/aced/projects/HOP
- /programs/aced/projects/Lung_Cancer
- /programs/aced/projects/MCF10A
- /programs/aced/projects/NVIDIA
- /programs/aced/projects/Prostate_Cancer
- /programs/aced/projects/ohsu_download_testing
```

> I need to see what buckets are associated with the commons

```
$ gen3_util buckets ls
buckets:
  GS_BUCKETS: {}
  S3_BUCKETS:
    aced-default:
      endpoint_url: https://minio-default.compbio.ohsu.edu
      region: us-east-1
    aced-manchester:
      endpoint_url: https://minio-manchester.compbio.ohsu.edu
      region: us-east-1
    aced-ohsu:
      endpoint_url: https://minio-ohsu.compbio.ohsu.edu
      region: us-east-1
    aced-stanford:
      endpoint_url: https://minio-stanford.compbio.ohsu.edu
      region: us-east-1
    aced-ucl:
      endpoint_url: https://minio-ucl.compbio.ohsu.edu
      region: us-east-1
endpoint: https://aced-training.compbio.ohsu.edu
msg: OK


```

> I need to create a project

```text
$ gen3_util projects touch aced-MyExperiment
projects:
  aced-MyExperiment:
    exists: true
messages:
- Created program:aced Program is updated!

```

> I need to assign default policies to that project

```text
$ gen3_util projects add policies aced-MyExperiment
msg: Approve these requests to assign default policies to aced-MyExperiment
commands:
- gen3_util access update 24f047d7-0e7c-43c6-bab6-61e2d385c71a SIGNED
- gen3_util access update 293c6cd1-7ab7-420f-bafb-34319589eac4 SIGNED

```

> I need to add a user to that project

```text
$ gen3_util projects add user aced-MyExperiment linus.pauling@osu.edu
msg: Approve these requests to add linus.pauling@osu.edu to aced-MyExperiment
commands:
- gen3_util access update 293c6cd1-7ab7-420f-bafb-34319589eac4 SIGNED

```

> Before proceeding, I need to sign those equests

```text
gen3_util access update xxxxxx SIGNED
```



> I want to create a simple project structure with a set of files

```text
$ gen3_util meta  import dir tests/fixtures/dir_to_study/ tmp/foo --project_id aced-MyExperiment
summary:
  ResearchStudy:
    count: 1
  DocumentReference:
    count: 5
    size: 6013814
msg: OK

```

> I want need to do something a bit more complex, for example, I want to create a project structure with a set of files, but I need to specify the `Patient` and `Specimen` based on the path of the file.

```text
gen3_util meta  import dir tests/fixtures/dir_to_study_with_meta/ tmp/foometa --project_id aced-foometa --plugin_path ./tests/unit/plugins

tests/fixtures/dir_to_study_with_meta/
├── file-2.csv
├── p1
│   ├── s1
│   │   └── file-3.pdf
│   ├── s2
│   │   └── file-4.tsv
│   └── s3
│       └── file-5
└── p2
    └── s4
        └── file-1.txt

Will produce the following meta data:

summary:
  ResearchStudy:
    count: 1
  Patient:
    count: 2
  Specimen:
    count: 4
  DocumentReference:
    count: 5
    size: 6013814

```

For more see [test_meta_plugin](./tests/unit/meta/test_plugins.py)



> I need to upload those files to the instance

```
$ gen3_util files cp --ignore_state --project_id aced-MyExperiment tmp/foo/DocumentReference.ndjson  bucket://aced-development-ohsu-data-bucket
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████| 5.74M/5.74M [00:03<00:00, 1.71MB/s, elapsed=0:00:02.056022, file=6f8101]
info:
- Wrote state to ~/.gen3/gen3-util-state/state.ndjson
msg: OK
```


> I need to upload the meta data about those files to the instance

```
$gen3_util meta cp tmp/foo bucket://aced-development-ohsu-data-bucket --project_id aced-MyExperiment
msg: Uploaded /var/folders/2c/hffqqtr94nv64tjy0xrl38r89k1sty/T/tmpacozhhoo/_aced-MyExperiment_meta.zip
```


> I need to request or manage access to a project

```
$ gen3_util access
Usage: gen3_util access [OPTIONS] COMMAND [ARGS]...

  Manage access requests.

Options:
  --help  Show this message and exit.

Commands:
  touch   Create a request for read access.
  update  Update the request's approval workflow.
  ls      List current user's requests.
  cat     Show details of a specific request.

```





## Development Setup

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .
```

## Test

* fixtures - data for testing environment

```
tests/fixtures/
└── custom_config
    └── config.yaml  # testing configuration

```

* test parameters

```
tests/
├── integration
│   └── conftest.py
└── unit
    └── conftest.py
```

* running tests

```

$ pytest --cov=gen3_util

 88%


```

* pre commit tests

A reasonable set of checks, including running unit tests prior to each commit.  You can run these tests on demand by:

```
$ pre-commit install

$ pre-commit run --all-files
debug statements (python)................................................Passed
check python ast.........................................................Passed
fix utf-8 byte order marker..............................................Passed
check json...........................................(no files to check)Skipped
detect private key.......................................................Passed
check yaml...............................................................Passed
check for added large files..............................................Passed
check that scripts with shebangs are executable..........................Passed
check for case conflicts.................................................Passed
fix end of files.........................................................Passed
trim trailing whitespace.................................................Passed
mixed line ending........................................................Passed
run our unit tests.......................................................Passed

```

## Distribution

- PyPi

```
# update pypi

# pypi credentials - see https://twine.readthedocs.io/en/stable/#environment-variables

export TWINE_USERNAME=  #  the username to use for authentication to the repository.
export TWINE_PASSWORD=  # the password to use for authentication to the repository.

# this could be maintained as so: export $(cat .env | xargs)

rm -r dist/
python3  setup.py sdist bdist_wheel
twine upload dist/*
```
