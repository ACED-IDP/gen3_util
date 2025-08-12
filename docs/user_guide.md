# User Guide for G3T CLI

G3T CLI is a command-line interface for adding version control to Gen3 projects. It provides a set of commands to manage and interact with your Gen3 projects.

## Installation

G3T CLI is a Python package and can be installed using pip:

```bash
pip install g3t
```

## Basic Usage

To use the G3T CLI, you can run the `g3t` command in your terminal. The basic syntax is:

```bash
g3t [OPTIONS] COMMAND [ARGS]...
```

## Options

- `--format`: Specifies the output format. It can be 'yaml', 'json'.  The default is 'yaml'.
- `--profile`: Specifies the connection name. See https://bit.ly/3NbKGi4 for more details.
- `--debug`: Enables debug mode.
- `--dry-run`: Prints the commands that would be executed, but does not execute them.

## Commands

The G3T CLI provides several commands for managing your Gen3 projects:
- `init`: Initialize a new repository for version control.
- `add`: Update references to data files in the repository.
- `status`: Show the changed files in the repository.
- `meta`: Manage the META directory, which contains information on the study, subjects, specimens.
- `commit`: Commit changes to the repository.
- `push`: Push changes to the remote repository.

Additional commands can be added to the G3T CLI based on your specific use case. Some examples include:
-  clone`: Clone a repository into a new directory.
- `collaborator`: Manage project membership, including adding and removing collaborators.
- `project`: Add this command to your `g3t` command to manage your Gen3 projects.

## Use Cases

1. **Creating a project**: To create a new Gen3 project, you can use the `init` command. For example:

    ```bash
    g3t --profile local <init> --project_id <program>-<project>
    ```
    A privileged user can approve a project with the `--approve` flag.

2. **Adding a Collaborator**: To add a collaborator to a Gen3 project, you can use the `collaborator` command. For example:

    ```bash
    g3t collaborator add --email collaborator@example.com
    ```
    To add a collaborator with write access, you can use the `--write` flag.
    A privileged user can approve the collaborator with the `--approve` flag.

3. **Adding a Data File**: To add a data file to a Gen3 project, you can use the `add` command. For example:
  Note: Data files are external to the project and the system maintains a mirror in the MANIFEST directory.

    ```bash
    # add a data file from a local path
    g3t add  path/to/data/file
    # add a file from a URL
    g3t add https://example.com/data/file --md5 <md5sum> --size <size> --modified <modified>
    ```

5. **Committing a Data File**: To commit a data file in a Gen3 project, you can use the `data` command. For example:

    ```bash
    g3t commit -m "a message describing the change" file1 file2 ...
    ```


Please note that the actual commands and their usage might vary based on the actual implementation of the G3T CLI. Always refer to the help documentation provided by the CLI for the most accurate information.

## Administrator Commands

The G3T CLI provides additional commands for administrators to manage Gen3 projects.

* A data steward can approve requests to add collaborators to a project. The `add-steward` command is used to add a data steward to a program. The command requires the user's email and the program path as arguments.
* A sysadmin can add a data steward to a data-access-committee member (data steward). The `add-steward` command is used to add a data steward to a program. The command requires the user's email and the program path as arguments.

```shell
g3t collaborator add-steward  --help
Usage: g3t collaborator add-steward [OPTIONS] USER_NAME RESOURCE_PATH

  Add a data steward user with approval rights to a program.

  USER_NAME (str): user's email
  RESOURCE_PATH (str): Gen3 authz /programs/<program>

Options:
  -a, --approve  Approve the addition (privileged)
  --help         Show this message and exit.

```
