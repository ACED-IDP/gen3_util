# User Stories

Follow these docs for steps on project creation, permissions access requests, permisions access approval, metadata generation, and file bucket uploading.

# Exacloud Python Environment Setup
You need python >= 3.9 to run the gen3_util. Check with:
```
python --version
```
## Pyenv Installation
If you do not have a Python 3.9 environment and aren't sure how to get one on Exacloud here is one method:

Check for an already existing pyenv with:
```
ls ~/.pyenv
```
Remove the pyenv if .pyenv directory exists.
```
Cd ~
srun rm -rf .pyenv
logout
```
Log back into exacloud

### Clone Pyenv GitHub repository

```
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
```
* Checking out files: 100% (1088/1088), done.

Go to pyenv directory and build it
```
cd ~/.pyenv && src/configure && make -C src
```
* make: Entering directory `/home/users/{your_username}/.pyenv/src'
gcc -fPIC     -c -o realpath.o realpath.c
gcc -shared -Wl,-soname,../libexec/pyenv-realpath.dylib  -o ../libexec/pyenv-realpath.dylib realpath.o
make: Leaving directory `/home/users/{your_username}/.pyenv/src'

Append pyenv variables to path and then add them to your
.bashrc so that they don’t go away after a logout

```
echo 'export PATH="~/.pyenv/bin:$PATH"' >> ~/.bashrc
echo  'eval "$(pyenv init -)"' >> ~/.bashrc
```
### Install desired Python version
```
pyenv install 3.9
```
* Downloading Python-3.9.17.tar.xz...
-> https://www.python.org/ftp/python/3.9.17/Python-3.9.17.tar.xz
Installing Python-3.9.17...
Installed Python-3.9.17 to /home/users/peterkor/.pyenv/versions/3.9.17
```
cd ..
pyenv global 3.9
python --version
```
* Python 3.9.17

# Credentials
In order to use the gen3_util you need to generate a credentials file from https://staging.aced-idp.org/identity
and store it in ~/.gen3 directory on whatever machine you are using the gen3_util on.

## Copying credentials file from your local computer downloads folder to Exacloud:

Go to https://staging.aced-idp.org/identity, press "Create Api key" in the top lefthand corner of the screen and then press
download json, orange button in the popup menu

Open a terminal session and run the below commands but replace {your_ohsu_username} with your ohsu username, without braces surounding it.

```
cd ~/Downloads
scp credentials.json {your_ohsu_username}@acc.ohsu.edu:/home/users/{your_ohsu_username}

ssh {your_ohsu_username}@acc.ohsu.edu

scp credentials.json {your_ohsu_username}@exahead1.ohsu.edu:/home/users/{your_ohsu_username}/.gen3
```
Note: this assumes that you already have a directory on exacloud in the location: home/users/{your_ohsu_username}/
called .gen3

## Virtual Environment Setup
Follow the intstructions in the [README.md](https://github.com/ACED-IDP/gen3_util/blob/main/README.md#installation)

# Setting up a project and uploading files to Gen3

A project can be created with:
```
gen3_util projects touch aced-whatever_name_you_want
```

Permissions can be requested from a project with:
```
gen3_util access touch {you_user_name}@ohsu.edu --project_id aced-whatever_name_you_chose --roles "storage_writer,file_uploader,indexd_admin"
```
Note: The --roles option is used to specify if you are requesting additional project permissions or not. For example if you just need read permissions, this flag is not needed, but if you need to write data to buckets you need to include these flags.

You will then see an output that will look like:
```
request:
  resource_id: null
  policy_id: programs.aced.projects.fullnew_storage_writer_file_uploader
  updated_time: '2023-06-26T17:46:46.542052'
  revoke: false
  status: DRAFT
  request_id: 389d5a32-09a8-4b9f-9604-2bcdfdbeef03
  resource_display_name: null
  username: you_user_name@ohsu.edu
  created_time: '2023-06-26T17:46:31.240850'
msg: OK
```
copy the request_id field for use in the next command

If you do not have admin permissions you will not be able to run the next command, and might need to contact someone who has admin permissions. 

## Approving new project requests

An access request's default status on creation is DRAFT. 

The "Update" Command is used to approve or reject the requested permissions specified in the "gen3_util access touch" command.

```
gen3_util access update your_request_id_pasted_here SIGNED
```

## Generating meta data for files to be uploaded

To generate meta data that is required for upload, run the folling command

```
gen3_util meta import dir DATA/ FHIR/ --project_id aced-whatever_project_name_you_chose
```

Where DATA is the directory where your files to be uploaded are located and FHIR/ is the directory that house the meta data that is generated upon running this command

## Uploading files to aced buckets

Run the following command to upload the files from the directory specified in the meta data command with:

```
gen3_util files cp --project_id aced-whatever_name_you_chose FHIR/DocumentReference.ndjson  bucket://aced-development-ohsu-data-bucket
```

Alternatively you could run the below command and use a different bucket location listed:
```
gen3_util buckets ls
```
