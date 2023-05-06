
# Gen3 Utilities

Utilities to manage Gen3 schemas, projects and submissions.

## Installation

```

# optionally
$python3 -m venv venv ; source venv/bin/activate

pip install gen3_util

gen3_util
>>> {'msg': 'Version 0.0.1'}

```


## Use

```
$gen3_util --help

Usage: gen3_util [OPTIONS] COMMAND [ARGS]...

  Gen3 Management Utilities

Options:
  --config TEXT              Path to config file. GEN3_UTIL_CONFIG
  --format [yaml|json|text]  Result format. GEN3_UTIL_FORMAT
  --help                     Show this message and exit.

Commands:
  projects  Manage Gen3 projects.
  meta      Manage meta data.
  files     Manage file buckets.
  config    Configure this utility.

```

## Connectivity

* Leverages Gen3Auth  [See](https://uc-cdis.github.io/gen3-user-doc/appendices/api-gen3/#credentials-to-query-the-api.)
* Store the `credentials.json` file in ~/.gen3/credentials.json or specify location with either env[GEN3_API_KEY], or `--cred` parameter


```
$ gen3_util projects ping
msg: OK connected to endpoint https://aced-training.compbio.ohsu.edu

$ gen3_util projects ls
endpoint: https://aced-training.compbio.ohsu.edu
projects:
- /programs/aced/projects/Alcoholism
- /programs/aced/projects/Alzheimers
- /programs/aced/projects/Breast_Cancer
- /programs/aced/projects/Colon_Cancer
- /programs/aced/projects/Diabetes
- /programs/aced/projects/Lung_Cancer
- /programs/aced/projects/Prostate_Cancer


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

$ pytest

tests/integration/test_project.py ...
tests/unit/test_cli.py ......                                                                                                                                                                   [ 66%]
tests/unit/test_coding_conventions.py .                                                                                                                                                         [ 77%]
tests/unit/test_config.py ..

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

rm -r dist/
python3  setup.py sdist bdist_wheel
twine upload dist/*
```
