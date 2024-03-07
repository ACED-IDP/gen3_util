import pathlib
import os
from hashlib import md5
import click
import deepdiff
import orjson

from gen3_util.common import calc_md5


def filter_files(files: list[str], ignore: list[str] = ['.DS_Store']):
    """Filter out files."""
    return [f for f in files if f not in ignore]


def walk_dir(path: pathlib.Path, ignore: list[str]):
    """Walk a directory, return a list of file names, sizes, md5 in relative path order."""
    paths = []
    for directory, subdir, files in os.walk(str(path), followlinks=True):
        directory = directory.replace(str(path), '')
        if directory.startswith('/'):
            directory = directory[1:]
        directory = pathlib.Path(directory)
        for file_name in files:
            if file_name not in ignore:
                file_path = pathlib.Path(str(path), str(directory), file_name)
                assert file_path.exists(), (path, directory, file_name, str(file_path))
                stats = os.stat(file_path)
                md5_hash = calc_md5(file_path, md5())
                paths.append(str(directory/file_name) + f"?size={stats.st_size}&md5={md5_hash.hexdigest()}")
    return sorted(paths)


def find_file_difference(mine, theirs):
    with open(mine, 'r') as my_fp, open(theirs, 'r') as their_fp:
        # assumes files already sorted
        line = 0
        for my_content, their_content in zip(my_fp.readlines(), their_fp.readlines()):
            line += 1
            my_md5 = md5(orjson.dumps(orjson.loads(my_content), option=orjson.OPT_SORT_KEYS)).hexdigest()
            their_md5 = md5(orjson.dumps(orjson.loads(their_content), option=orjson.OPT_SORT_KEYS)).hexdigest()
            if my_md5 != their_md5:
                # Use DeepDiff to find the difference
                diff = deepdiff.DeepDiff(my_content, their_content)
                # Print or process the differences
                click.echo(f"Differences between the two files on line {line}:", diff)


def diff_dir(theirs: pathlib.Path, mine: pathlib.Path, ignore: list[str] = ['.DS_Store']):
    """Return the difference between two directories."""
    their_files = walk_dir(theirs, ignore)
    my_files = walk_dir(mine, ignore)
    deep_diff = deepdiff.DeepDiff(their_files, my_files)
    if 'values_changed' in deep_diff:
        click.echo(deep_diff['values_changed'])
        for value_change in deep_diff['values_changed'].values():
            new_value = value_change.pop('new_value').split('?')[0]
            old_value = value_change.pop('old_value').split('?')[0]
            my_path = mine.joinpath(new_value)
            their_path = theirs.joinpath(old_value)
            click.echo(my_path, their_path)
            find_file_difference(my_path, their_path)
    return deep_diff


# main
if __name__ == "__main__":
    def _main():
        mine = pathlib.Path('tests2')
        theirs = pathlib.Path('tests')
        click.echo(diff_dir(theirs, mine))
    _main()
