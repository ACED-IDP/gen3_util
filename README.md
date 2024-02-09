
![](docs/gen3_tracker-logo.png)
# Gen3 Tracker

Utilities to manage Gen3 schemas, projects and submissions.

## Quick Start
### Installation
```

$ pip install gen3_tracker

$ g3t version
version: 0.0.1


```


### Use

```
$ g3t --help
Usage: g3t [OPTIONS] COMMAND [ARGS]...

  Gen3 Tracker: manage FHIR metadata and files.

Options:
  --format [yaml|json|text]  Result format. G3T_FORMAT  [default: yaml]
  --profile TEXT             Connection name. G3T_PROFILE See
                             https://bit.ly/3NbKGi4

  --version
  --help                     Show this message and exit.

Commands:
  ping          Verify gen3-client and test connectivity.
  init          Create project, both locally and on remote.
  add           Add file to the index.
  commit        Record changes to the project.
  diff          Show new/changed metadata since last commit.
  push          Submit committed changes to commons.
  status        Show the working tree status.
  clone         Clone meta and files from remote.
  pull          Download latest meta and data files.
  update-index  Update the index from the META directory.
  rm            Remove project.
  utilities     Useful utilities.


```

## User Guide
* See [use cases and documentation](https://aced-idp.github.io/)

## Contributing
* See [CONTRIBUTING.md](CONTRIBUTING.md)
