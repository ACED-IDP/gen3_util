
# G3T workflow

## install g3t

```bash
pip install gen3_util
```

## configure g3t

> See gen3-client

The g3t will use the `--profile` argument or following environment variables to determine the remote server:
```
export G3T_PROFILE=local
```


## Initialization

The g3t init command is used to initialize a new repository in a directory. When you run this command, g3t creates a new subdirectory within the existing directory that houses the internal data structure required for version control. Here's a brief explanation of what happens when you use g3t init:

Initialization: Running g3t init initializes a new Git repository in the current directory. It doesn't affect your existing files; instead, it adds:
* a hidden subfolder `.g3t` within your project that houses the internal data structure required for version control.
* a visible subfolder `META` within your project that houses the FHIR metadata files.

Existing Files: If you run g3t init in a directory that already contains files, g3t will not overwrite them.

```g3t init
g3t init --help
Usage: g3t init [OPTIONS]

  Create project, both locally and on remote.

Options:
  --project_id TEXT  Gen3 program-project G3T_PROJECT_ID

```

Note the program-project is significant.  It is used to determine the location of the remote repository, bucket storage and access control.  Contact support for more on supported program and project names.

While you can work with an initialized repository locally, **an authorized user will need to sign** the project request before you can push to the remote repository. See `g3t utilities access sign`


### `META` directory

The convention of using a `META` directory for supporting files a common practice in data management. This directory is used to organize and store the metadata files of your project that describe the Study, Subjects, Specimens, Documents, etc. . Here's a brief explanation of this convention:

* Separation of concerns: The `META` directory provides a clear separation between your metadata and other project files. This helps maintain a clean and organized project structure.
* Clarity and Readability: By placing metadata files in a dedicated directory, it becomes easier for researchers (including yourself and others) to locate and understand the main codebase. This improves overall project clarity and readability.
* Build Tools Integration: Many build tools and development environments are configured by default to recognize the `META` directory as the main metadata location. This convention simplifies the configuration process and ensures that tools can easily identify and analyze your data files.
* Consistency Across Projects: Adopting a common convention, such as using `META` for metadata files, promotes consistency across different projects. When researchers work on multiple projects, having a consistent structure makes it easier to navigate and understand each study.
* The 'META' directory will contain files FHIR resource name with the extension [.ndjson](https://www.hl7.org/fhir/nd-json.html). e.g. `ResearchStudy.ndjson`

### Data directories

All data directories are relative to the root of the project. You have flexibility as to their names and hierarchy. However, they must be relative to the root of the project. Here's a brief explanation of this convention:

* Portability: Relative paths make your project more portable, meaning that it can be moved to different locations or shared with others without causing issues with file references. This is particularly important in data engineering projects where datasets and files may be stored in different locations.
* Ease of Collaboration: When working on a data engineering project with multiple team members, using relative paths ensures that everyone can run the code without having to modify file paths based on their local directory structure. This promotes smoother collaboration.
* Consistency Across Environments: Data engineering projects often involve processing large datasets, and the code needs to run consistently across different environments (e.g., development, testing, production). Relative paths help maintain this consistency by allowing the code to reference files and directories relative to the project's root.

```
 g3t add --help
Usage: g3t add [OPTIONS] LOCAL_PATH

  Add file to the index.

  local_path: relative path to file on local file system

Options:
  --specimen_id TEXT     fhir specimen identifier
  --patient_id TEXT      fhir patient identifier
  --task_id TEXT         fhir task identifier
  --observation_id TEXT  fhir observation identifier
  --md5 TEXT             MD5 sum, if not provided, will be calculated before
                         upload
```

#### Migration of existing project data

If you have an existing project that you want to migrate using g3t, you can do so by following these steps:
* Create a new repository using g3t init.
* Either move or copy your existing data files into the new repository.  Alternatively, g3t create Symbolic links are supported.
*

## Workflow

### add data files

```
 g3t add --help
Usage: g3t add [OPTIONS] LOCAL_PATH

  Add file to the index.

  local_path: relative path to file or symbolic link on the local file system

Options:
  --specimen_id TEXT     fhir specimen identifier
  --patient_id TEXT      fhir patient identifier
  --task_id TEXT         fhir task identifier
  --observation_id TEXT  fhir observation identifier
  --md5 TEXT             MD5 sum, if not provided, will be calculated before
                         upload

```

### Create metadata files

Every file uploaded to the project must have accompanying metadata in the form of FHIR resources.  The metadata is stored in the `META` directory.
* The minimum required metadata for a study is the `ResearchStudy` resource.
* The minimum required metadata for a file is the `DocumentReference` resource.

Additional resources can be added to the metadata files:
* subjects (ResearchSubject, Patient)
* specimens (Specimen)
* assays (Task, DiagnosticReport)
* measurements (Observation)

As a convenience, the `g3t utilities meta create` command will create a minimal metadata for each file in the project.
* This command will create a skeleton metadata file for each file added to the project using the `_id` parameters specified on the `g3t add` command.
* You can edit the metadata to map additional fields.
* The metadata files can be created at any time, but the system will validate them before the changes are committed.


## Committing Changes

The g3t commit command is used to save your changes to the local repository. Here's a brief explanation of what happens when you use g3t commit:
* Files in the META directory are validated and evaluated for consistency.
* A change set of metadata records are created.
* The commit is saved to the local repository.

```
 g3t commit --help
Usage: g3t commit [OPTIONS] [METADATA_PATH]

  Record changes to the project.

  METADATA_PATH: directory containing metadata files to be committed. [default: ./META]

Options:
  -m, --message TEXT  Use the given <msg> as the commit message.  [required]

```
## Viewing the Changes to be committed

The `g3t status` command is used to view the commit history of a project. Here's a brief explanation of what happens when you use g3t log:
* a list of commits including commit_id, message, files and resource_counts.
* a list of files added to the project, but not yet committed.
* a summary of resource counts on the remote repository.

```
 g3t status --help
Usage: g3t status [OPTIONS]

  Show the working tree status.
```

## Pushing Changes

The `g3t push` command is used to upload your changes to the remote repository. Here's a brief explanation of what happens when you use g3t push:

* Each commit is transferred to the remote repository.
* All files are uploaded to the project bucket.
* A `publish` job is started on the remote server to process the changes.
* Job status can be polled with the `g3t status` command.
  * A job status is returned, the more details job's id can be monitored with the `g3t utilities jobs get <uid>` command.
* Once the job is complete:
  * The changes are available on the portal.
  * The changes are reflected in the `g3t status` command.
  * The changes are available to other users for download

```
 g3t push --help
Usage: g3t push [OPTIONS]

  Submit committed changes to commons.

Options:
  --overwrite  overwrite files records in index  [default: False]

```

## Cloning a Project

The `g3t clone` command is used to clone a project from the remote repository. Here's a brief explanation of what happens when you use g3t clone:
* A subdirectory is created for the project, it is named after the `project_id`.
* The project is initialized locally, including the `.g3t` and `META` directories.
* The current metadata is downloaded from the remote repository.
* By default, data files are not downloaded by default:
  * Use the `--data_type all` option to specify `all` files will be downloaded.

```shell
g3t clone --help
Usage: g3t clone [OPTIONS]

  Clone meta and files from remote.

Options:
  --project_id TEXT             Gen3 program-project G3T_PROJECT_ID
  --data_type [meta|files|all]  Clone meta and/or files from remote.
                                [default: meta]

```

## submitter test script

```bash
# Use case: As a data submitter, I will need to create a project.
## test should work with or without environment variables
#export G3T_PROFILE=local
#export G3T_PROJECT_ID=test-test002b
#g3t init
unset G3T_PROJECT_ID
unset G3T_PROFILE
g3t --profile local init --project_id test-test001b

# Use case: As a institution data steward, I need to approve the project before it can be shared.
g3t utilities access sign

# Use case: As a data submitter, I will need to add files to the project and associate them with a subject(patient).
g3t add tests/fixtures/dir_to_study/file-1.txt  --patient_id P1
g3t utilities meta create
## test meta generation:  META should have 4 files
g3t commit  -m "commit-1"
## test that the commit: g3t status should return commit info - was message added?
#  resource_counts:
#      DocumentReference: 1
#      Patient: 1
#      ResearchStudy: 1
#     ResearchSubject: 1

# Use case: when subjects are added to study I need to add them to the project.
g3t add tests/fixtures/dir_to_study/file-2.csv  --patient_id P2
g3t status
## test add: should return one entry in "uncommitted_manifest:"
g3t utilities meta create
## test meta generation:  META should have 4 files Patient ResearchSubject DocumentReference should have 1 new record each
g3t commit -m "commit-2"
## test the commit: g3t status should return commit info - was message added? there should only be the three new records
#    resource_counts:
#      DocumentReference: 1
#      Patient: 1
#      ResearchSubject: 1
#    manifest_files:
#    - tests/fixtures/dir_to_study/file-2.csv

# Use case: some subjects have specimens, I need to add them to the project.
g3t add tests/fixtures/dir_to_study/sub-dir/file-3.pdf --patient_id P3 --specimen_id S3
g3t utilities meta create
## test should create a Specimen.ndjson file in META
# Created 4 new records.
wc -l META/Specimen.ndjson
#       1 META/Specimen.ndjson
g3t commit -m "commit-3"
## test the commit: g3t status should return commit info - was message added? 4 new records
#    message: commit-3
#    resource_counts:
#      DocumentReference: 1
#      Patient: 1
#      ResearchSubject: 1
#      Specimen: 1
#    manifest_files:
#    - tests/fixtures/dir_to_study/sub-dir/file-3.pdf

# Use case: I'm ready to share my data
## push to remote
g3t push
## test:  the system should respond with reasonable, informative messages without too much verbosity
## I need to know the status of my project. During job execution, I should be able to query the status.
g3t status
## test: After job execution, I should have detailed information about the results.
#  pushed_commits:
#  - published_timestamp: 2024-01-19T09:45:47.018426
#    published_job:
#      output:
#        uid: 82322961-8d2a-47e4-8833-af0e299aa393
#        name: fhir-import-export-ohiwi
#        status: Completed
#    commits:
#    - d050c8f931bab152279ff18e0a21434f commit-1
#    - 2f77cf6017ec3b0485b7493ebe459f53 commit-2
#    - a550281b43713937ce684e3cab13639f commit-3

## test: Once complete, the remote counts should reconcile with my activity
#remote:
#  resource_counts:
#    DocumentReference: 3
#    Patient: 3
#    ResearchStudy: 1
#    ResearchSubject: 3
#    Specimen: 1
wc -l META/*.ndjson
#       3 META/DocumentReference.ndjson
#       3 META/Patient.ndjson
#       1 META/ResearchStudy.ndjson
#       3 META/ResearchSubject.ndjson
#       1 META/Specimen.ndjson

## If I want more detailed information, I should be able to query it
## get UID from status -> local.pushed_commits.published_job.output.uid
g3t utilities jobs get UID
# ....


# Use case: As a data submitter, when I know more about meta, I should be able to add it.
# e.g. alter a patient record
sed -i.bak 's/"P1"}]}/"P1"}], "gender": "male"}/' META/Patient.ndjson
# see https://stackoverflow.com/a/22084103
rm META/Patient.ndjson.bak
g3t commit -m "commit-4"
## test: the commit should process only one patient record
#resource_counts:
#  Patient: 1

## Use case: I should be able to publish a 'meta only' change
g3t push

## Use case: As a human being, I make mistakes, the system should prevent me from committing `no changes`
g3t commit -m "commit-5 has no changes"
## test: the system should reject the commit
# msg: No resources changed in META

## Use case: As a human being, I make mistakes, the system should prevent me from committing `invalid fhir`
sed -i.bak 's/"gender"/"foobar"/' META/Patient.ndjson
# see https://stackoverflow.com/a/22084103
rm META/Patient.ndjson.bak
g3t commit -m "commit-6 has invalid fhir"
## test: should fail validation, the response should be informative and give me enough information to fix the problem


```

## consumer test script
```shell
# Use case: As a data consumer, I will need download a project.

## test should work with or without environment variables

#export G3T_PROFILE=local
#export G3T_PROJECT_ID=test-test002b
#g3t clone

unset G3T_PROJECT_ID
unset G3T_PROFILE
g3t --profile local clone --project_id test-test001b

## test: the project should exist
cd test-test001b
## test: the meta data should be in place with the latest changes
grep male META/Patient.ndjson |  jq '[.id, .gender]'
#"20d7d7eb-46f9-5175-b474-cb504f66e10e"
## test by default, the files should not be downloaded
ls tests
# ls: tests: No such file or directory

## Use case: I should be able to download files
g3t pull
## test directory should now contain
tree tests
#tests
#└── fixtures
#    └── dir_to_study
#        ├── file-1.txt
#        ├── file-2.csv
#        └── sub-dir
#            └── file-3.pdf
#

```
