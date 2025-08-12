
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
#### Optional: install the dtale package for interactive data exploration
* called from `g3t meta dataframe --dtale`
```
pip install g3t[dtale].
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
  init          Initialize a new repository.
  add           Update references to data files to the repository.
  status        Show changed files.
  push          Push changes to the remote repository.
  pull          Fetch from and integrate with a remote repository.
  clone         Clone a repository into a new directory
  ls            List files in the repository.
  rm            Remove a single file from the server index, and MANIFEST.
  ping          Verify gen3-client and test connectivity.
  meta          Manage the META directory.
  collaborator  Manage project membership.
  projects      Manage Gen3 projects.



```

## User Guide
* See [use cases and documentation](https://aced-idp.github.io/)

## Contributing
* See [CONTRIBUTING.md](CONTRIBUTING.md)
