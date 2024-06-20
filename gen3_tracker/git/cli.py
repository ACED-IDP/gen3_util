import glob
import json
import logging
import multiprocessing
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import datetime

import click
import pytz
import yaml
from fhir.resources.documentreference import DocumentReference
from gen3.auth import Gen3AuthError
from gen3.file import Gen3File
from gen3.index import Gen3Index
from halo import Halo
from tqdm import tqdm

import gen3_tracker
from gen3_tracker import Config
from gen3_tracker.common import CLIOutput, INFO_COLOR, ERROR_COLOR, is_url, filter_dicts, SUCCESS_COLOR, \
    read_ndjson_file
from gen3_tracker.config import init as config_init
from gen3_tracker.config import init as config_init, ensure_auth
from gen3_tracker.git import git_files, to_indexd, to_remote, dvc_data, \
    data_file_changes, modified_date, git_status, DVC, MISSING_G3T_MESSAGE
from gen3_tracker.git import run_command, \
    MISSING_GIT_MESSAGE, git_repository_exists
from gen3_tracker.git.adder import url_path, write_dvc_file
from gen3_tracker.git.cloner import ls
from gen3_tracker.git.initializer import initialize_project_server_side
from gen3_tracker.git.snapshotter import push_snapshot
from gen3_tracker.meta.skeleton import meta_index

# logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__package__)


# @click.command(cls=NaturalOrderGroup)
# @click.option('--debug', is_flag=True, envvar='G3T_DEBUG', help='Enable debug mode. G3T_DEBUG environment variable can also be used.')
# @click.option('--dry-run', is_flag=True, envvar='G3T_DRYRUN', help='Print the commands that would be executed, but do not execute them. G3T_DRYRUN environment variable can also be used.')
# @click.version_option()
# @click.pass_obj
# def cli(ctx: click.Context, debug: bool, dry_run: bool):
#     """[Experimental] A CLI for adding version control to Gen3 projects."""
#
#     config.ensure_object(dict)
#     config.debug = debug
#     config.debug = dry_run
#     if debug:
#         _logger.setLevel(logging.DEBUG)

def _check_parameters(config, project_id):
    """Common parameter checks."""
    if not project_id:
        raise AssertionError("project_id is required")
    if not project_id.count('-') == 1:
        raise AssertionError(f"project_id must be of the form program-project {project_id}")
    if not config.gen3.profile:
        click.secho("No profile set. Continuing in disconnected mode. Use `set profile <profile>`", fg='yellow')


@click.group(cls=gen3_tracker.NaturalOrderGroup)
def cli():
    """git-like version control for Gen3 projects."""
    pass


@cli.command(context_settings=dict(ignore_unknown_options=True))
# @click.option('--force', '-f', is_flag=True, help='Force the init.')
@click.argument('project_id', default=None, required=False, envvar=f"{gen3_tracker.ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--approve', '-a', help='Approve the addition (privileged)', is_flag=True, default=False, show_default=True)
@click.option('--no-server', help='Skip server setup (testing)', is_flag=True, default=False, show_default=True, hidden=True)
@click.pass_obj
def init(config: Config, project_id: str, approve: bool, no_server: bool):
    """Initialize a new repository."""
    try:
        # uncomment if we want to check for a git remote
        # try:
        #     assert not git_remote_exists(config.debug), 'A git remote exists. PHI and large data files should not be stored in a public repository. Use the --force option to continue.'
        # except Exception as e:
        #     if not force:
        #         click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        #         if config.debug:
        #             raise

        # click.secho(INIT_MESSAGE, fg=INFO_COLOR, file=sys.stderr)

        _check_parameters(config, project_id)

        logs = []
        # create directories
        for _ in config_init(config, project_id):
            logs.append(_)

        ensure_git_repo(config)

        if not no_server:
            logs.extend(initialize_project_server_side(config, project_id))

            if approve:
                run_command('g3t projects create', dry_run=config.dry_run, no_capture=True)
                run_command('g3t collaborator approve --all', dry_run=config.dry_run, no_capture=True)
            else:
                click.secho("To approve the project, a privileged user must run `g3t projects create` and `g3t collaborator approve --all`", fg=INFO_COLOR, file=sys.stderr)

        if config.debug:
            for _ in logs:
                click.secho(_, fg=INFO_COLOR, file=sys.stderr)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


def ensure_git_repo(config):
    # ensure a git repo
    if pathlib.Path('.git').exists():
        return

    if not pathlib.Path('.git').exists():
        command = 'git init'
        run_command(command, dry_run=config.dry_run, no_capture=True)
    else:
        click.secho('Git repository already exists.', fg=INFO_COLOR, file=sys.stderr)
    pathlib.Path('MANIFEST').mkdir(exist_ok=True)
    pathlib.Path('META').mkdir(exist_ok=True)
    pathlib.Path('LOGS').mkdir(exist_ok=True)
    with open('.gitignore', 'w') as f:
        f.write('LOGS/\n')
        f.write('.g3t/state/\n')  # legacy
    with open('META/README.md', 'w') as f:
        f.write('This directory contains metadata files for the data files in the MANIFEST directory.\n')
    with open('MANIFEST/README.md', 'w') as f:
        f.write('This directory contains dvc files that reference the data files.\n')
    run_command('git add MANIFEST META .gitignore .g3t', dry_run=config.dry_run, no_capture=True)
    run_command('git commit -m "initialized" MANIFEST META .gitignore .g3t', dry_run=config.dry_run, no_capture=True)


# Note: The commented code below is an example of how to use context settings to allow extra arguments.
# context_settings=dict(
#     ignore_unknown_options=True,
#     allow_extra_args=True,
# )


@cli.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.argument('target')
@click.option('--no-git-add', default=False, is_flag=True, hidden=True)
@click.pass_context
def add(ctx, target, no_git_add: bool):
    """
    Update references to data files to the repository.

    \b
    TARGET is a "data file", either files or urls.
    We commit their proxies - MANIFEST/<TARGET>.dvc files.
    \b
    If the TARGET is a file in the local file system:
        - We will automatically calculate the hash, size, modified and mime type of the file
        - You can specify those values with the --<hash>, --size, --modified and --mime options
        - As a convenience, you can use wildcards to add multiple files at once.
          If wildcards are used, the hash, size, modified and mime type parameters are ignored.
    \b
    If the TARGET is a url:
        - You must specify the hash, size, modified and mime type
        - Wildcards are not supported
    \b
    --<hash> <value>: Valid options are: ['md5', 'sha1', 'sha256', 'sha512', 'crc', 'etag']
                      Value must conform to the hash type.
    --modified: A variety of date formats are supported, see https://tinyurl.com/ysad3rj7
    --mime: If not specified, it will be inferred from the file extension.
    --no-bucket: If specified, the file will not be uploaded to the bucket, and user will access via scp or symlink.
    --no-git-add: If specified, the file will not be automatically added to git. Avoids locking, useful for parallel adds .
    \b
    Identifiers:
    In order to link a file with associated Patient, Specimen, Observation or Task, you can use one of the following identifiers:
    --patient <value>: The patient identifier
    --specimen <value>: The specimen identifier, requires patient
    --observation <value>: The observation identifier, requires patient, optionally specimen
    --task <value>: The task identifier, requires either patient or specimen
    The <value> is a user defined string that will be used to link the data file with the associated resource.  Do not use PHI in the value.
    See `g3t meta` for more
    """
    from gen3_tracker.git.adder import add_file, add_url

    config: Config = ctx.obj
    try:
        # needs to be in project root
        assert git_repository_exists(config.debug), MISSING_GIT_MESSAGE
        assert not config.no_config_found, MISSING_G3T_MESSAGE

        # needs to have a target
        assert target, 'No targets specified.'

        # Expand wildcard paths
        if is_url(target) and not target.startswith('file://'):
            all_changed_files, updates = add_url(ctx, target)
        else:
            all_changed_files, updates = add_file(ctx, target)

        #
        # if it is an update, we do not need to add the file to git
        #
        adds = [str(_) for _ in all_changed_files if _ not in updates]
        if adds and not no_git_add:
            adds.append('.gitignore')
            run_command(f'git add {" ".join([str(_) for _ in adds])}', dry_run=config.dry_run, no_capture=True)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


@cli.command(context_settings=dict(ignore_unknown_options=True, allow_extra_args=True))
@click.argument('targets',  nargs=-1)
@click.option('--message', '-m', help='The commit message.')
@click.option('--all', '-a', is_flag=True, default=False, help='Automatically stage files that have been modified and deleted.')
@click.pass_context
def commit(ctx, targets, message, all):
    """Commit the changes
    \b
    TARGETS: list of files (default: all)
    """
    config: Config = ctx.obj
    command = [
        "git",
        "commit",
        "-m",
        f'"{message}"',
    ] + list(targets)

    if all:
        command.append("-a")

    run_command(" ".join(command), dry_run=config.dry_run, no_capture=True)


@cli.command()
@click.pass_obj
def status(config):
    """Show changed files."""
    soft_error = False
    try:
        with Halo(text='Scanning', spinner='line', placement='right', color='white'):
            manifest_path = pathlib.Path('MANIFEST')
            changes = data_file_changes(manifest_path)
            # Get a list of all files in the MANIFEST directory and its subdirectories
            files = glob.glob('MANIFEST/**/*.dvc', recursive=True)
            # Filter out directories, keep only files
            files = [f for f in files if os.path.isfile(f)]
        if not files:
            click.secho(f"No files have been added.", fg=INFO_COLOR, file=sys.stderr)
        else:
            # Find the most recently changed file
            latest_file = max(files, key=os.path.getmtime)

            document_reference_mtime = 0

            if pathlib.Path('META/DocumentReference.ndjson').exists():
                # Get the modification time
                document_reference_mtime = os.path.getmtime('META/DocumentReference.ndjson')

            latest_file_mtime = os.path.getmtime(latest_file)
            if document_reference_mtime < latest_file_mtime:
                document_reference_mtime = datetime.fromtimestamp(document_reference_mtime).isoformat()
                latest_file_mtime = datetime.fromtimestamp(latest_file_mtime).isoformat()
                click.secho(f"WARNING: DocumentReference.ndjson is out of date {document_reference_mtime}. The most recently changed file is {latest_file} {latest_file_mtime}.  Please check DocumentReferences.ndjson", fg=INFO_COLOR, file=sys.stderr)
                soft_error = True

            if changes:
                click.secho(f"# There are {len(changes)} data files that you need to update via `g3t add`:", fg=INFO_COLOR, file=sys.stderr)
                cwd = pathlib.Path.cwd()
                for _ in changes:
                    data_path = str(_.data_path).replace(str(cwd) + '/', "")
                    click.secho(f'  g3t add {data_path} # changed: {modified_date(_.data_path)},  last added: {modified_date(_.dvc_path)}', fg=INFO_COLOR, file=sys.stderr)
                    soft_error = True
            else:
                click.secho("No data file changes.", fg=INFO_COLOR, file=sys.stderr)

        _ = run_command('git status')
        print(_.stdout)
        if soft_error:
            exit(1)
    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


@cli.command()
@click.option('--step',
              type=click.Choice(['index', 'upload', 'publish', 'all']),
              default='all',
              show_default=True,
              help='The step to run '
              )
@click.option('--transfer-method',
              type=click.Choice(gen3_tracker.FILE_TRANSFER_METHODS.keys()),
              default='gen3',
              show_default=True,
              help='The upload method.'
              )
@click.option('--overwrite', is_flag=True, help='(index): Overwrite previously submitted files.')
@click.option('--wait', default=True, is_flag=True, show_default=True, help="(publish): Wait for metadata completion.")
@click.option('--dry-run', show_default=True, default=False, is_flag=True, help='Print the commands that would be executed, but do not execute them.')
@click.option('--re-run', show_default=True, default=False, is_flag=True, help='Re-run the last publish step')
@click.pass_context
def push(ctx, step: str, transfer_method: str, overwrite: bool, re_run: bool, wait: bool, dry_run: bool):
    """Push changes to the remote repository.

    \b
    steps:
        index - index the files
        push - push the files to the remote
        publish - publish the files to the portal
        all - all of the above.
    transfer-method: specify the remote storage type:
        gen3 - gen3-client to/from local
        no-bucket - indexd only symlink to/from local
        s3 - (admin) s3 to/from local
        s3-map - (admin) s3 index only external s3
    """
    from gen3_tracker.gen3.jobs import publish_commits
    from gen3_tracker.gen3.buckets import get_program_bucket

    config = ctx.obj

    try:

        if re_run:
            # step = 'publish'
            raise NotImplementedError("Re-run not implemented")

        try:
            with Halo(text='Checking', spinner='line', placement='right', color='white'):
                run_command("g3t status")
        except Exception as e:
            click.secho("Please correct issues before pushing.", fg=ERROR_COLOR, file=sys.stderr)
            click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
            if config.debug:
                raise
            exit(1)

        with Halo(text='Scanning', spinner='line', placement='right', color='white'):

            # check git status
            branch, uncommitted = git_status()
            assert not uncommitted, f"Uncommitted changes found.  Please commit or stash them first."

            # check dvc vs external files
            changes = data_file_changes(pathlib.Path('MANIFEST'))
            assert not changes, f"# There are {len(changes)} data files that you need to update.  See `g3t status`"

            # initialize dvc objects with this project_id
            committed_files, dvc_objects = manifest(config.gen3.project_id)

            # initialize gen3 client
            auth = gen3_tracker.config.ensure_auth(config=config)
            bucket_name = get_program_bucket(config=config, auth=auth)

            # check for new files
            records = ls(config, metadata={'project_id': config.gen3.project_id}, auth=auth)['records']
            dids = {_['did']: _['updated_date'] for _ in records}
            new_dvc_objects = [_ for _ in dvc_objects if _.object_id not in dids]
            updated_dvc_objects = [_ for _ in dvc_objects if _.object_id in dids and _.out.modified > dids[_.object_id]]
            if step != 'publish':
                if not overwrite:
                    dvc_objects = new_dvc_objects + updated_dvc_objects
                    assert dvc_objects, f"No new files to index.  Use --overwrite to force"

        click.secho(f'Scanned new: {len(new_dvc_objects)}, updated: {len(updated_dvc_objects)} files', fg=INFO_COLOR, file=sys.stderr)
        if updated_dvc_objects:
            click.secho(f'Found {len(updated_dvc_objects)} updated files. overwriting', fg=INFO_COLOR, file=sys.stderr)
            overwrite = True

        if step in ['index', 'all']:
            # send to index

            if dry_run:
                click.secho("Dry run: not indexing files", fg=INFO_COLOR, file=sys.stderr)
                yaml.dump(
                    {
                        'new': [_.model_dump() for _ in new_dvc_objects],
                        'updated': [_.model_dump() for _ in updated_dvc_objects],
                    },
                    sys.stdout
                )
                return

            for _ in tqdm(
                    to_indexd(
                        dvc_objects=dvc_objects,
                        auth=auth,
                        project_id=config.gen3.project_id,
                        bucket_name=bucket_name,
                        overwrite=overwrite,
                        restricted_project_id=None

                    ),
                    desc='Indexing', unit='file', leave=False, total=len(committed_files)):
                pass
            click.secho(f'Indexed {len(committed_files)} files.', fg=INFO_COLOR, file=sys.stderr)

        if step in ['upload', 'all']:

            click.secho(f'Checking {len(dvc_objects)} files for upload via {transfer_method}', fg=INFO_COLOR, file=sys.stderr)
            to_remote(
                upload_method=transfer_method,
                dvc_objects=dvc_objects,
                bucket_name=bucket_name,
                profile=config.gen3.profile,
                dry_run=config.dry_run,
                work_dir=config.work_dir
            )

        with Halo(text='Uploading snapshot', spinner='line', placement='right', color='white'):
            # push the snapshot of the `.git` sub-directory in the current directory
            push_snapshot(config, auth=auth)

        if step in ['publish', 'all']:
            if transfer_method == 'gen3':
                with Halo(text='Publishing', spinner='line', placement='right', color='white') as spinner:
                    # legacy, "old" fhir_import_export use publish_commits to publish the META
                    _ = publish_commits(config, wait=wait, auth=auth, bucket_name=bucket_name, spinner=spinner)
                click.secho(f'Published project. See logs/publish.log', fg=SUCCESS_COLOR, file=sys.stderr)
                with open("logs/publish.log", 'a') as f:
                    log_msg = {'timestamp': datetime.now(pytz.UTC).isoformat()}
                    log_msg.update(_)
                    f.write(json.dumps(log_msg, separators=(',', ':')))
                    f.write('\n')
            else:
                click.secho(f'Auto-publishing not supported for {transfer_method}. Please use --step publish after uploading', fg=ERROR_COLOR, file=sys.stderr)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise
        exit(1)


def manifest(project_id) -> tuple[list[str], list[DVC]]:
    """Get the committed files and their dvc objects. Initialize dvc objects with this project_id"""
    committed_files = [_ for _ in git_files() if _.endswith('.dvc')]
    dvc_objects = [_ for _ in dvc_data(committed_files)]
    for _ in dvc_objects:
        _.project_id = project_id
    return committed_files, dvc_objects


@cli.command()
@click.option('--remote',
              type=click.Choice(['gen3', 's3', 'ln', 'scp']),
              default='gen3',
              show_default=True,
              help='Specify the remote storage type. gen3:download, s3:s3 cp, ln: symbolic link, scp: scp copy'
              )
@click.option('--worker_count', '-w', default=(multiprocessing.cpu_count() - 1), show_default=True,
              type=int,
              help='Number of workers to use.')
@click.option('--data-only', help='Ignore git snapshot', is_flag=True, default=False, show_default=True)
@click.pass_obj
def pull(config: Config, remote: str, worker_count: int, data_only: bool):
    """ Fetch from and integrate with a remote repository."""
    try:
        from gen3_tracker.git.cloner import find_latest_snapshot, ls

        with Halo(text='Authorizing', spinner='line', placement='right', color='white'):
            auth = gen3_tracker.config.ensure_auth(config=config)

        if not data_only:
            with Halo(text='Pulling git snapshot', spinner='line', placement='right', color='white'):
                if not auth:
                    auth = gen3_tracker.config.ensure_auth(config=config)
                snapshot, zip_filepath = download_snapshot(auth, config)
                # Get the current timestamp
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                # Define the new directory name
                new_dir_name = config.work_dir / f"git-backup-{timestamp}"
                # Rename the directory
                shutil.move(".git", new_dir_name)
                # unzip the snapshot
                with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                    zip_ref.extractall('.')
            click.secho(f"Pulled {snapshot['file_name']}", fg=INFO_COLOR, file=sys.stderr)

        manifest_files, dvc_objects = manifest(config.gen3.project_id)
        if remote == 'gen3':
            # download the files
            with Halo(text='Pulling from gen3', spinner='line', placement='right', color='white'):
                object_ids = [{'object_id': _.object_id} for _ in dvc_objects]  # if not _.out.source_url
                current_time = datetime.now().strftime("%Y%m%d%H%M%S")  # Format datetime as you need
                manifest_file = pathlib.Path(config.work_dir) / f'manifest-{current_time}.json'
                with open(manifest_file, 'w') as fp:
                    json.dump(object_ids, fp)
            cmd = f'gen3-client download-multiple --no-prompt --profile {config.gen3.profile}  --manifest {manifest_file} --numparallel {worker_count}'
            print(cmd)
            run_command(cmd, no_capture=True)
        elif remote == 's3':
            with Halo(text='Pulling from s3', spinner='line', placement='right', color='white'):
                if not auth:
                    auth = gen3_tracker.config.ensure_auth(config=config)
                results = ls(config, metadata={'project_id': config.gen3.project_id}, auth=auth)
                object_ids = [_.object_id for _ in dvc_objects]
            for _ in results['records']:
                if _['did'] in object_ids:
                    print('aws s3 cp ', _['urls'][0], _['file_name'])
        elif remote == 'ln':
            for _ in dvc_objects:
                print(f"ln -s {_.out.realpath} {_.out.path}")
        elif remote == 'scp':
            for _ in dvc_objects:
                print(f"scp USER@HOST:{_.out.realpath} {_.out.path}")

        else:
            raise NotImplementedError(f"Remote {remote} not supported.")

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


@cli.command()
@click.argument('project_id', default=None, required=False, envvar=f"{gen3_tracker.ENV_VARIABLE_PREFIX}PROJECT_ID", metavar='PROJECT_ID')
@click.pass_obj
def clone(config, project_id):
    """Clone a repository into a new directory"""
    try:
        config.gen3.project_id = project_id
        assert not pathlib.Path(project_id).exists(), f"{project_id} already exists.  Please remove it first."
        os.mkdir(project_id)
        os.chdir(project_id)
        with Halo(text='Cloning', spinner='line', placement='right', color='white'):
            auth = gen3_tracker.config.ensure_auth(config=config)
            snapshot, zip_filepath = download_snapshot(auth, config)
            assert not pathlib.Path('.git').exists(), "A git repository already exists.  Please remove it, or move to another directory first."
            # unzip
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall('.')

            # if we just unzipped a .git these directories will exist
            expected_dirs = ['.git', 'META', 'MANIFEST']
            if not all([pathlib.Path(_).exists() for _ in expected_dirs]):
                # if not, we have downloaded a legacy SNAPSHOT.zip, so lets migrate the data to the expected drirectories
                click.secho(f"{expected_dirs} not found after downloading {snapshot['file_name']} processing legacy snapshot", fg=INFO_COLOR, file=sys.stderr)
                # legacy - was this a *SNAPSHOT.zip?
                meta_files = (pathlib.Path('studies') / config.gen3.project)
                # legacy - was this a *meta.zip?
                if not meta_files.exists():
                    meta_files = pathlib.Path('.')
                # create local directories and git
                [_ for _ in config_init(config, project_id)]
                ensure_git_repo(config=config)
                # move ndjson from studies to META
                for _ in meta_files.glob('*.ndjson'):
                    shutil.move(_, 'META/')
                # add to git
                run_command('git add META/*.*')
                # migrate DocumentReferences to MANIFEST
                references = meta_index()
                manifest_files = []
                for _ in read_ndjson_file('META/DocumentReference.ndjson'):
                    document_reference = DocumentReference.parse_obj(_)
                    dvc_object = DVC.from_document_reference(config, document_reference, references)
                    manifest_files.append(write_dvc_file(yaml_data=dvc_object.model_dump(), target=dvc_object.out.path))

                # Get the current time in seconds since the epoch
                current_time = time.time()
                # Update the access and modification times of the file
                os.utime('META/DocumentReference.ndjson', (current_time, current_time))

                run_command('git add MANIFEST/')
                run_command('git commit -m "migrated from legacy" MANIFEST/ META/ .gitignore')
                shutil.move(zip_filepath, config.work_dir / zip_filepath.name)

        click.secho(f"Cloned {snapshot['file_name']}", fg=INFO_COLOR, file=sys.stderr)
        run_command("git status", no_capture=True)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


def download_snapshot(auth, config):
    """Download the latest snapshot."""
    from gen3_tracker.git.cloner import find_latest_snapshot
    snapshot = find_latest_snapshot(auth, config)

    gen3_file = Gen3File(auth)
    pathlib.Path(snapshot['file_name']).parent.mkdir(exist_ok=True, parents=True)
    ok = gen3_file.download_single(snapshot['did'], '.')
    assert ok, f"Failed to download {snapshot['did']}"

    zip_filepath = pathlib.Path(snapshot['file_name'])
    assert zip_filepath.exists(), f"Failed to download {snapshot['did']}"
    return snapshot, zip_filepath


def file_name_or_guid(config, object_id) -> (str, pathlib.Path):
    """Check if the object_id is a file name or a GUID."""
    guid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
    path = None
    if not guid_pattern.match(object_id):
        if not is_url(object_id):
            path = pathlib.Path('MANIFEST') / (object_id + ".dvc")
        else:
            path = pathlib.Path('MANIFEST') / (url_path(object_id) + ".dvc")

        if path.exists():
            dvc_object = next(iter(dvc_data([str(path)])), None)
            assert dvc_object, f"{object_id} is not a valid GUID or DVC file."
            dvc_object.project_id = config.gen3.project_id
            object_id = dvc_object.object_id
        else:
            raise ValueError(
                f"{object_id} was not found in the MANIFEST and does not appear to be an object identifier (GUID).")
    else:
        committed_files, dvc_objects = manifest(config.gen3.project_id)
        dvc_objects = [dvc_object for dvc_object in dvc_objects if dvc_object.object_id == object_id]
        assert dvc_objects, f"{object_id} not found in MANIFEST."
        path = pathlib.Path('MANIFEST') / (dvc_objects[0].out.path + ".dvc")

    assert guid_pattern.match(object_id), f"{object_id} was not found in MANIFEST."
    return object_id, path


@cli.command("ls")
@click.option('--long', '-l', 'long_flag', default=False, is_flag=True, help='Long listing format.', show_default=True)
@click.argument('target', default=None, required=False)
@click.pass_obj
def ls_cli(config: Config, long_flag: bool, target: str):
    """List files in the repository.
    \b
    TARGET wild card match of guid, path or hash.
    """
    try:
        from gen3_tracker.git.cloner import find_latest_snapshot, ls

        with Halo(text='Pulling file list', spinner='line', placement='right', color='white'):
            auth = gen3_tracker.config.ensure_auth(config=config)
            results = ls(config, metadata={'project_id': config.gen3.project_id}, auth=auth)
            indexd_records = results['records']
            committed_files, dvc_objects = manifest(config.gen3.project_id)
            # list all data files
            dvc_objects = {_.object_id: _ for _ in dvc_objects}

            def _dvc_meta(dvc_object, full=False) -> dict:
                if not dvc_object:
                    return {}
                _ = {}
                if not full and dvc_object.meta:
                    for k, v in dvc_object.meta.model_dump(exclude_none=True).items():
                        if v:
                            _[k] = v
                else:
                    _ = dvc_object.model_dump(exclude_none=True)
                _['object_id'] = dvc_object.object_id
                return _

            if not long_flag:
                indexd_records = [
                    {
                        'did': _['did'],
                        'file_name': _['file_name'],
                        'indexd_created_date': _['created_date'],
                        'meta': _dvc_meta(dvc_objects.get(_['did'], None)),
                        'urls': _['urls']
                     } for _ in indexd_records
                ]

        bucket_ids = {_['did'] for _ in indexd_records}

        uncommitted = pathlib.Path('MANIFEST').glob('**/*.dvc')
        uncommitted = [str(_) for _ in uncommitted]
        uncommitted = [str(_) for _ in uncommitted if _ not in committed_files]
        uncommitted = [_.model_dump(exclude_none=True) for _ in dvc_data(uncommitted)]

        _ = {
            'bucket': indexd_records,
            'committed': [_dvc_meta(v, full=True) for k, v in dvc_objects.items() if k not in bucket_ids],
            'uncommitted': uncommitted
        }

        if target:
            # Escape special characters and replace wildcard '*' with '.*' for regex pattern
            pattern = re.escape(target).replace("\\*", ".*")
            filtered = {
                'bucket': filter_dicts(_.get('bucket', []), pattern),
                'committed': filter_dicts(_.get('committed', []), pattern),
                'uncommitted': filter_dicts(_.get('uncommitted', []), pattern)
            }
            _ = filtered

        if config.output.format == 'json':
            print(json.dumps(_, indent=2))
        else:
            yaml.dump(_, sys.stdout, default_flow_style=False)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


@cli.command()
@click.argument('object_id', metavar='<name>')
@click.pass_obj
def rm(config: Config, object_id: str):
    """Remove a single file from the server index, and MANIFEST. Does not alter META.
    \b
    <name> is a GUID or a data file name.
    """
    try:

        with Halo(text='Searching', spinner='line', placement='right', color='white'):
            object_id, path = file_name_or_guid(config, object_id)

        with Halo(text='Deleting from server', spinner='line', placement='right', color='white'):
            auth = gen3_tracker.config.ensure_auth(config=config)
            index = Gen3Index(auth)
            result = index.delete_record(object_id)
        if not result:
            if not path:
                path = ''
            click.secho(f"Failed to delete {object_id} from server. {path}", fg=ERROR_COLOR, file=sys.stderr)
        else:
            click.secho(f"Deleted {object_id} from server. {path}", fg=INFO_COLOR, file=sys.stderr)

        with Halo(text='Scanning', spinner='line', placement='right', color='white'):
            committed_files, dvc_objects = manifest(config.gen3.project_id)
            dvc_objects = [dvc_object for dvc_object in dvc_objects if dvc_object.object_id == object_id]
            assert dvc_objects, f"{object_id} not found in MANIFEST."
            dvc_object = dvc_objects[0]
            path = pathlib.Path('MANIFEST') / (dvc_object.out.path + ".dvc")
            assert path.exists(), f"{path} not found"
            path.unlink()
        click.secho(f"Deleted {path} from MANIFEST. Please adjust META resources", fg=INFO_COLOR, file=sys.stderr)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


@cli.command(name="ping")
@click.pass_obj
def ping(config: Config):
    """Verify gen3-client and test connectivity."""
    with CLIOutput(config=config) as output:
        msgs = []
        ok = True
        cmd = "gen3-client --version".split()
        gen3_client_installed = subprocess.run(cmd, capture_output=True)
        if gen3_client_installed.returncode != 0:
            msgs.append("gen3-client not installed")
            ok = False

        gen_client_ini_file = gen3_tracker.config.gen_client_ini_path()
        auth = None
        if not gen_client_ini_file.exists():
            msgs.append("not configured")
            ok = False
        else:
            try:
                assert config.gen3.profile, "No profile found"
                auth = ensure_auth(config=config, validate=True)
                assert auth, "Authentication failed"
                msgs.append(f"Connected using profile:{config.gen3.profile}")
            except (AssertionError, ValueError) as e:
                msgs.append(str(e))
                ok = False
            except Gen3AuthError as e:
                msg = str(e).split(':')[0]
                msgs.append(msg)
                msg2 = str(e).split('<p class="introduction">')[-1]
                msg2 = msg2.split('</p>')[0]
                msgs.append(msg2)
                ok = False

        if ok:
            _ = "Configuration OK: "
        else:
            _ = "Configuration ERROR: "
            output.exit_code = 1

        _ = {'msg': _ + ', '.join(msgs)}
        if auth:
            _['endpoint'] = auth.endpoint
            _['username'] = auth.curl('/user/user').json()['username']
        output.update(_)



# @cli.command()
# @click.argument('targets', nargs=-1)
# @click.pass_obj
# def diff(config: Config, targets):
#     """Show details of changed files."""
#     try:
#         click.secho(DIFF_MESSAGE, fg=INFO_COLOR, file=sys.stderr)
#         changes = git_status(config.debug)
#
#         if not targets:
#             targets = []
#             for k in ['committed', 'uncommitted']:
#                 if k in changes:
#                     targets.extend(changes[k]['modified'])
#                     click.secho(f"Targets not specified, comparing {k} changes", fg=INFO_COLOR, file=sys.stderr)
#
#         if not targets:
#             click.secho("No changes found", fg=INFO_COLOR, file=sys.stderr)
#             return
#
#         # check committed changes
#         for _ in targets:
#             if _.endswith('.dvc'):
#                 continue
#             if _ in changes.get('committed', {'modified': []})['modified']:
#                 click.secho(f'{_} is in the committed changes.', fg=INFO_COLOR, file=sys.stderr)
#                 # read the git diff, get the hashes of the changed files
#                 results = run_command(f'git diff {_}.dvc', config.debug)
#                 hashes = [_.replace('md5:', '') for _ in results.stdout.split('\n') if 'md5:' in _]
#                 old = hashes[0].split()[-1]
#                 new = hashes[1].split()[-1]
#                 # there should be a change in the hash
#                 assert old != new, f'{_} has not changed.'
#                 # get the file paths from the dvc cache
#                 # see https://dvc.org/doc/user-guide/project-structure/internal-files#structure-of-the-cache-directory
#                 dvc_cache_path = pathlib.Path('.dvc/cache/files/md5').resolve()
#                 old_file = dvc_cache_path / old[:2] / old[2:]
#                 new_file = dvc_cache_path / new[:2] / new[2:]
#                 # run diff on the files
#                 results = run_command(f'diff -u {old_file} {new_file}', config.debug, raise_on_err=False)
#                 click.secho(_, fg=INFO_COLOR, file=sys.stderr)
#                 print(results.stdout)
#             else:
#                 # click.secho(f'{_} is in the uncommitted changes.', fg=INFO_COLOR, file=sys.stderr)
#                 # get the hash of the original file
#                 changes = dvc_diff_head(config.debug)
#                 modified = changes.get('modified', [])
#                 for modified_file in modified:
#                     if modified_file['path'] == _:
#                         old = modified_file['hash']['old']
#                         dvc_cache_path = pathlib.Path('.dvc/cache/files/md5').resolve()
#                         old_file = dvc_cache_path / old[:2] / old[2:]
#                         new_file = modified_file['path']
#                         # run diff on the files
#                         print(f'diff -u {old_file} {new_file}')
#                         results = run_command(f'diff -u {old_file} {new_file}', config.debug, raise_on_err=False)
#                         click.secho(_, fg=INFO_COLOR, file=sys.stderr)
#                         print(results.stdout)
#                         break
#
#     except Exception as e:
#         click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
#         if config.debug:
#             raise
#
#
# @cli.command()
# @click.argument('branch')
# @click.pass_obj
# def checkout(ctx: click.Context, branch: str):
#     """Checkout a particular branch or commit."""
#     try:
#         click.secho(f"Checking out {branch}", fg=INFO_COLOR, file=sys.stderr)
#         run_command(f'git checkout {branch}', config.debug, no_capture=True)
#         run_command(f'dvc checkout', config.debug, no_capture=True)
#     except Exception as e:
#         click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
#         if config.debug:
#             raise
