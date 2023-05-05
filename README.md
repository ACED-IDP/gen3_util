  
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

tests/unit/test_cli.py ......                                                                                                                                                                   [ 66%]
tests/unit/test_coding_conventions.py .                                                                                                                                                         [ 77%]
tests/unit/test_config.py ..             

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