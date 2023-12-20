
# Gen3 Utilities

Utilities to manage Gen3 schemas, projects and submissions.

## Installation
```

$ pip install gen3_util

$ gen3_util version
version: 0.0.12


```


### libmagic

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
  --profile TEXT             Connection name. GEN3_UTIL_PROFILE See
                             https://bit.ly/3NbKGi4

  --state_dir TEXT           Directory for file transfer state
                             GEN3_UTIL_STATE_DIR  [default: ~/.gen3/gen3_util]

  --help                     Show this message and exit.

Commands:
  projects  Manage Gen3 projects.
  buckets   Project buckets.
  meta      Manage meta data.
  files     Manage file transfers.
  access    Manage access requests.
  config    Configure this utility.
  jobs      Manage Gen3 jobs.
  users     Manage project membership.
  version   Print version
  ping      Test connectivity to Gen3 endpoint.


```

## Connectivity

* Uses [gen3-client](https://gen3.org/resources/user/gen3-client/#2-configure-a-profile-with-credentials) for authentication


## Use cases

> I need to verify connectivity.

```
$ gen3_util --profile <connection-name> ping
msg: 'Configuration OK: Connected using profile:production'
endpoint: https://aced-idp.org
username: user@example.com

```

> I need to see what projects exist

```
$ gen3_util projects ls

endpoint: https://aced-training.compbio.ohsu.edu
msg: OK
projects:
- /programs
- /programs/aced
- /programs/aced/projects
- /programs/aced/projects/Alcoholism
- /programs/aced/projects/Alzheimers
- /programs/aced/projects/Breast_Cancer
- /programs/aced/projects/Colon_Cancer
- /programs/aced/projects/Diabetes
- /programs/aced/projects/Lung_Cancer
- /programs/aced/projects/Prostate_Cancer
```

> I need to see what buckets are associated with the commons

```
$ gen3_util buckets ls
endpoint: https://aced-idp.org
buckets:
  GS_BUCKETS: {}
  S3_BUCKETS:
    <bucket-name>:
      endpoint_url: https://<example.com>/<bucket-name>
      programs:
      - <program-name>
      region: <region-name>

```

Note: the workflow to create new projects and adding users to those projects is a multi-step process.  The following commands will create the requests, but they will need to be signed by a user with approval privileges.  See requestor's ["Functionality and flow" documentation](https://github.com/uc-cdis/requestor/blob/master/docs/functionality_and_flow.md).

> I need to create a project

```text
$ gen3_util projects new --project_id=aced-my_new_project
requests:
- status: DRAFT
  policy_id: programs.aced.projects.my_new_project_writer
- status: DRAFT
  policy_id: programs.aced.projects.my_new_project_reader

```


> I need to add a user to that project

```text
$ gen3_util users add  --project_id=aced-my_new_project --username linus.pauling@osu.edu --write
requests:
- status: DRAFT
  username: linus.pauling@osu.edu
  policy_id: programs.aced.projects.my_new_project_writer
- status: DRAFT
  username: linus.pauling@osu.edu
  policy_id: programs.aced.projects.my_new_project_reader


```

> Before proceeding, a user with approval privileges will need to sign the requests

```text
gen3_util access sign
```

Note: Adding files to a project requires a multi-step process. In addition to uploading a file, associated metadata must be created and uploaded to the commons.  This will be done automatically for simple use cases, but may be overridden for more complex use cases such as bulk upload or prepared FHIR data.



> I want to create a simple project structure with a set of files

```text
Usage: gen3_util files manifest put [OPTIONS] LOCAL_PATH [REMOTE_PATH]

  Add file meta information to the manifest.

  local_path: path to file on local file system
  remote_path: name of the file in bucket, defaults to local_path

Options:
  --project_id TEXT      Gen3 program-project
  --specimen_id TEXT     fhir specimen identifier
  --patient_id TEXT      fhir patient identifier
  --task_id TEXT         fhir task identifier
  --observation_id TEXT  fhir observation identifier
  --md5 TEXT             MD5 sum, if not provided, will be calculated before
                         upload

  --help                 Show this message and exit.

```

<img width="673" alt="image" src="https://github.com/ACED-IDP/aced-idp.github.io/assets/47808/801cfb7a-bff3-4c4f-97be-acba79830787">

The following identifiers create a simple graph of the data from the DocumentReference(file) to its parents.

```text
  --project_id TEXT      Gen3 program-project [required]
  --specimen_id TEXT     fhir specimen identifier [optional]
  --patient_id TEXT      fhir patient identifier [optional]
  --task_id TEXT         fhir task identifier [optional]
  --observation_id TEXT  fhir observation identifier [optional]

```

Adding one or more commands via `gen3_util files manifest put` will create a manifest file that can be used to upload files and metadata to the commons.


> I need to upload those files to the instance, and automatically create meta data

```
$ gen3_util files manifest upload
```



## Development Setup

```
$ git clone git@github.com:ACED-IDP/gen3_util.git
$ cd gen3_util
$ python3 -m venv venv ; source venv/bin/activate
$ pip install -r requirements.txt
$ pip install -e .

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
