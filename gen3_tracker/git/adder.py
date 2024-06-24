import glob
import pathlib
import sys
from datetime import datetime
from urllib.parse import urlparse, ParseResult

import click
import dateutil
import pytz
import yaml
from dateutil.tz import tzutc

from gen3_tracker import Config
from gen3_tracker.common import ACCEPTABLE_HASHES
from gen3_tracker.git import DVC


def is_valid_url(target):
    try:
        result = urlparse(target)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


# handle http, https
def get_http_cloud_storage_path(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme not in ['http', 'https']:
        raise ValueError(f'Invalid scheme {parsed_url.scheme} for cloud storage URL')

    # The path component of the parsed URL starts with a '/', so we remove it with [1:]
    # For MinIO, the path includes the bucket name, so we need to split on '/' and take everything after the first element
    return '/'.join(parsed_url.path.split('/')[2:])


def get_http_bucket_from_url(url):
    parsed_url = urlparse(url)
    if parsed_url.scheme not in ['http', 'https']:
        raise ValueError(f'Invalid scheme {parsed_url.scheme} for cloud storage URL')

    # For MinIO, the bucket name is the first element in the path
    return parsed_url.path.split('/')[1]


# handle s3, gs, azure
def get_cloud_storage_path(url) -> ParseResult:
    """breaks the URL into components, which can then be used to derive the file path."""
    parsed_url: ParseResult = urlparse(url)
    if parsed_url.scheme not in ['s3', 'gs', 'azure']:
        raise ValueError(f'Invalid scheme {parsed_url.scheme} for cloud storage URL')

    # The path component of the parsed URL starts with a '/', so we remove it with [1:]
    return parsed_url


def add_url(ctx, target) -> tuple[list[pathlib.Path], list[str]]:
    from gen3_tracker.common import ACCEPTABLE_HASHES, INFO_COLOR

    """Add a url to the repository. If the file is already in the repository, it will be updated."""
    assert is_valid_url(target), f'{target} is not a valid url.'
    url = target
    from gen3_tracker.git import git_files

    files_already_in_repo = git_files()
    updates = []  # only updates
    all_changed_files = []  # new and updates
    config: Config = ctx.obj

    target = url_path(url)

    if f"MANIFEST/{target}.dvc" in files_already_in_repo:
        click.secho(f'{target} is already in the repository. updating.', fg=INFO_COLOR, file=sys.stderr)
        # flag the file for update
        updates.append(target)

    # process args to DVC
    _args = ctx.args
    metadata = {'meta': dict(map(lambda i: (_args[i].replace('--', ''), _args[i + 1]), range(len(_args) - 1)[::2]))}
    if '--no-bucket' in _args or '--no_bucket' in _args:
        metadata['meta']['no_bucket'] = True

    required_keys = ['size', 'modified']
    required_keys_msg = []
    for k in required_keys:
        if k not in metadata['meta']:
            required_keys_msg.append(f'--{k} is required for urls.')
    assert not required_keys_msg, f'{", ".join(required_keys_msg)}'

    # check content of parameters
    assert metadata['meta']['size'].isdigit(), 'size should be an integer.'
    metadata['meta']['size'] = int(metadata['meta']['size'])

    # assuming metadata['meta']['modified'] is a string representing a date
    date_str = metadata['meta']['modified']
    # parse the string into a datetime object
    date_obj = dateutil.parser.parse(date_str)
    # if the date string doesn't have a timezone, you can add one
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=tzutc())
    # convert the datetime object back into an ISO formatted string with timezone
    metadata['meta']['modified'] = date_obj.isoformat()

    for k in ACCEPTABLE_HASHES.keys():
        if k in metadata['meta']:
            hash_type = k
            hash_value = metadata['meta'][k]
            assert hash_value, f'{hash_type} should be provided.'
            hash_regex = ACCEPTABLE_HASHES[hash_type]
            assert hash_regex(hash_value), f'{hash_value} is not a valid {hash_type} hash.'
            if 'hash' in metadata['meta']:
                assert metadata['meta']['hash'] == hash_type, f'--hash {hash_type} does not match the provided hash type {metadata["meta"]["hash"]}.'
            else:
                metadata['meta']['hash'] = hash_type
            break

    # create reference to the file
    metadata['project_id'] = config.gen3.project_id
    yaml_data = create_dvc_for_url(metadata, target=target)
    yaml_data.update(metadata)
    yaml_data['outs'][0]['source_url'] = url
    # delete file keys from metadata
    for k in required_keys:
        del yaml_data['meta'][k]
    for k in ACCEPTABLE_HASHES.keys():
        if k in metadata['meta']:
            del yaml_data['meta'][k]

    # write the dvc file
    dvc_file = write_dvc_file(target, yaml_data)

    all_changed_files = [dvc_file]
    return all_changed_files, updates


def url_path(url):
    """Breaks the URL into components, which can then be used to derive the MANIFEST file path."""
    # The path component of the parsed URL starts with a '/', so we remove it with [1:]
    if not url.startswith('http'):
        cloud_storage_parts = get_cloud_storage_path(url)
        path = cloud_storage_parts.path[1:]
        bucket = cloud_storage_parts.netloc
    else:
        path = get_http_cloud_storage_path(url)
        bucket = get_http_bucket_from_url(url)
    target = f"{bucket}/{path}"
    return target


def add_file(ctx, target) -> tuple[list[pathlib.Path], list[str]]:
    from gen3_tracker.common import INFO_COLOR
    config: Config = ctx.obj

    """Add a real file to the repository. Expand wildcards. If the file is already in the repository, it will be updated."""
    from gen3_tracker.git import git_files

    targets = glob.glob(target, recursive=True)
    if not targets:
        # not a wildcard, check if it is a file
        # assert pathlib.Path(target).exists(), f'{pathlib.Path(target).resolve()} does not exist.'
        targets = [target]

    files_already_in_repo = git_files()
    updates = []  # only updates
    all_changed_files = []  # new and updates
    for target in targets:

        # check args & dependencies
        # target should be a file that exists and is relative to the project root
        if target.startswith('MANIFEST') or target.startswith('META'):
            suggested_name = target.replace('MANIFEST/', '').replace('META/', '').replace('.dvc', '')
            click.secho(f'{target} starts with a reserved name. Perhaps you meant {suggested_name}?', fg=INFO_COLOR,
                        file=sys.stderr)
            continue

        if f"MANIFEST/{target}.dvc" in files_already_in_repo:
            click.secho(f'{target} is already in the repository. updating.', fg=INFO_COLOR, file=sys.stderr)
            #
            # flag the file for update
            #
            updates.append(target)

        target_path = pathlib.Path(target)
        # assert not target_path.is_dir(), 'Adding directories are not supported. Please supply a file.'
        if target_path.is_dir():
            continue

        # final checks
        # assert target_path.resolve().exists(), f'{pathlib.Path(target).resolve()} does not exist.'
        assert target_path.resolve().is_relative_to(pathlib.Path.cwd()), 'Target should be relative to the project root.'

        # create reference to the file
        # convert --arguments to metadata
        _args = ctx.args
        metadata = {'meta': dict(map(lambda i: (_args[i].replace('--', ''), _args[i + 1]), range(len(_args) - 1)[::2]))}
        if '--no-bucket' in _args or '--no_bucket' in _args:
            metadata['meta']['no_bucket'] = True

        if 'hash' in metadata['meta']:
            hash_type = metadata['meta']['hash']
            assert hash_type in ACCEPTABLE_HASHES.keys(), f'hash should be one of {", ".join(ACCEPTABLE_HASHES.keys())}'
            hash_value = metadata['meta'][hash_type]
            assert hash_value, f'{hash_type} should be provided.'
            hash_regex = ACCEPTABLE_HASHES[hash_type]
            assert hash_regex(hash_value), f'{hash_value} is not a valid {hash_type} hash.'

        yaml_data = create_dvc(metadata, target_path)
        yaml_data.update(metadata)
        yaml_data['project_id'] = config.gen3.project_id
        _ = DVC(**yaml_data).model_dump()
        assert _['outs'][0]['object_id'], 'object_id should be in dvc created from files.'
        dvc_file = write_dvc_file(target, _)

        # add the target root to the gitignore
        git_ignore_path = pathlib.Path.cwd() / '.gitignore'
        all_changed_files.append(dvc_file)
        ignores = []
        if not git_ignore_path.exists():
            all_changed_files.append(git_ignore_path)
        else:
            with open(git_ignore_path) as f:
                ignores = f.readlines()

        target_root = f'/{target_path.parts[0]}\n'
        if target_root not in ignores:
            with open(git_ignore_path, 'a+') as f:
                f.write(target_root)

    return all_changed_files, updates


def write_dvc_file(target, yaml_data):
    # write the dvc file
    manifest_path = pathlib.Path('MANIFEST')
    # meta_path = pathlib.Path('META')
    manifest_target_path = manifest_path / target
    manifest_target_path.parent.mkdir(parents=True, exist_ok=True)
    dvc_file = manifest_target_path.parent / (manifest_target_path.name + ".dvc")
    if dvc_file.exists():
        with open(dvc_file) as f:
            existing_yaml_data = yaml.load(f, Loader=yaml.SafeLoader)
            # don't overwrite existing metadata with empty metadata
            for k, v in existing_yaml_data['meta'].items():
                if v is None:
                    continue
                if k in yaml_data['meta'] and yaml_data['meta'][k] is not None:
                    continue
                yaml_data['meta'][k] = v
    with open(dvc_file, 'w') as yaml_file:
        yaml.dump(yaml_data, yaml_file, default_flow_style=False)
    return dvc_file


def create_dvc(metadata: dict, target_path: pathlib.Path) -> dict:
    from gen3_tracker.common import ACCEPTABLE_HASHES

    """Create a dvc file for a file in the repository."""
    from gen3_tracker.git import calculate_hash, get_mime_type

    target = str(target_path)

    info = {
        'size': metadata['meta'].get('size', None) or target_path.stat().st_size,
        'path': target,
        'mime': metadata['meta'].get('mime', get_mime_type(target)),
        'modified': metadata['meta'].get('modified', None) or datetime.fromtimestamp(target_path.stat().st_mtime, tz=pytz.utc).isoformat(),
        'realpath': str(target_path.resolve()),
        'is_symlink': target_path.is_symlink()
    }
    if 'realpath' in metadata['meta']:
        info['realpath'] = metadata['meta']['realpath']
        del metadata['meta']['realpath']
    # did they provide a hash?

    for k in ACCEPTABLE_HASHES.keys():
        if k in metadata['meta']:
            info[k] = metadata['meta'][k]
            info['hash'] = k
            del metadata['meta'][k]
            if 'hash' in metadata['meta']:
                del metadata['meta']['hash']
    # no?, use md5
    if 'hash' not in info:
        info['hash'] = 'md5'
        info['md5'] = calculate_hash('md5', target)
    # we follow this convention for the dvc file
    # see https://dvc.org/doc/user-guide/project-structure/dvc-files#dvc-files
    yaml_data = {
        'outs': [info]
    }
    return yaml_data


def create_dvc_for_url(metadata: dict, target: str) -> dict:
    """Create a dvc file for a url in the repository."""
    from gen3_tracker.git import get_mime_type
    from gen3_tracker.common import ACCEPTABLE_HASHES

    info = {
        'size': metadata['meta'].get('size', None),
        'path': target,
        'mime': metadata['meta'].get('mime', get_mime_type(target)),
        'modified': metadata['meta'].get('modified', None),
        'realpath': None,
        'is_symlink': False
    }
    # did they provide a hash?
    for k in ACCEPTABLE_HASHES.keys():
        if k in metadata['meta']:
            info[k] = metadata['meta'][k]
            info['hash'] = k
    # we follow this convention for the dvc file
    # see https://dvc.org/doc/user-guide/project-structure/dvc-files#dvc-files
    yaml_data = {
        'project_id': metadata['project_id'],
        'outs': [info]
    }
    _ = DVC(**yaml_data).model_dump()
    assert _['outs'][0]['object_id'], f'object_id should be in dvc created for url. {metadata}'
    return _
