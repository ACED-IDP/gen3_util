
### `access` module that interfaces w/ Gen3's [requestor](https://github.com/uc-cdis/requestor)

> How gen3_util access works with Requestor service

#### Enables `User` and `Someone who makes access decisions`

> From Requestor's documentation

![image](https://github.com/uc-cdis/requestor/raw/master/docs/img/requestor_example_flow.png)


specifically:

* g3t utilities access `add` USER_NAME RESOURCE_PATH implements `Fill out access request form`
* g3t utilities access `sign` REQUEST_ID STATUS implements `update access request status`
* g3t utilities  access `ls` implements `send form data`



see https://github.com/uc-cdis/requestor/blob/master/docs/functionality_and_flow.md

---/---


```
$ g3t utilities access --help
Usage: g3t utilities access [OPTIONS] COMMAND [ARGS]...

  Manage access requests.

Options:
  --help  Show this message and exit.

Commands:
  add   Create a request a specific role.
  sign  Sign all policies for a project.
  ls    List current user's requests.
  cat   Show details of a specific request.

```

## Approving projects test script

In order to add a data steward,a user with authorization to approve projects, to the system, adapt the 'test script' above as follows:

```shell
# As a data steward, I need to know un-approved users can't approve projects

# As an approved user, create a project, do __not__ sign it
g3t --profile local init --project_id ohsu-test001b

# Using a gmail address not used elsewhere in the system, log in to the portal and create a profile file
# Register that token with gen3-client, here we use the profile name `local-steward`
# Attempt to sign the project
g3t --profile local-steward utilities access sign
## expected output
# msg: No unsigned requests found
```

## Adding users who can approve projects


```shell
# As an admin, in order to delegate approvals for the 'create project' and 'add user' use cases, I need to give permissions to users.

## As an admin, add the requester reader and updater role on a particular program to an un privileged user
g3t utilities access add data_steward_example@<institution>.edu --resource_path /programs/<institution>/projects  --roles requestor_updater_role
g3t utilities access add data_steward_example@<institution>.edu --resource_path /programs/<institution>/projects  --roles requestor_reader_role
# As an admin, approve that request
g3t utilities access sign

# As an admin, create a project, do __not__ sign it
g3t init --project_id <institution>-<any_project>

# As a data steward, login to the portal and create a profile file, configure gen3-client with the profile name `local-steward`
g3t --profile local-steward utilities access sign
## The steward should be able to sign the project
## The project resource now exists in arborist

# As a ACED administrator, I need to create projects in sheepdog so that submissions can take place.
g3t utilities projects ls
## test: the project should be listed as incomplete
g3t utilities projects create
## test: the project should be listed as complete

```
