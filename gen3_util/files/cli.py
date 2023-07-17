import click

from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.common import is_url
from gen3_util.config import Config
from gen3_util.files.downloader import cp as download
from gen3_util.files.lister import ls
from gen3_util.files.remover import rm
from gen3_util.files.uploader import cp as upload


@click.group(name='files', cls=NaturalOrderGroup)
@click.pass_obj
def file_group(config):
    """Manage file transfers."""
    pass


@file_group.command(name="ls")
@click.pass_obj
def files_ls(config: Config):
    """List files in a project."""
    ls(config)


@file_group.command(name="cp")
@click.argument('from_', )
@click.argument('to_')
@click.option('--worker_count', default=10, show_default=True,
              help="Number of worker processes")
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, required=True, show_default=True,
              help="Gen3 program-project")
@click.option('--source_path', required=False, default=None, show_default=True,
              help='Path on local file system')
@click.option('--disable_progress_bar', default=False, is_flag=True, show_default=True,
              help="Show progress bar")
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True,
              help="Re-write metadata from indexd")
@click.pass_obj
def files_cp(config: Config, from_: str, to_: str, worker_count: int, ignore_state: bool, project_id: str,
             source_path: str, disable_progress_bar: bool, duplicate_check: bool):
    """Copy files to/from the project bucket.

    \b
    from_: Source url or path
    to_: Destination url or path
    """

    with CLIOutput(config=config) as output:

        if is_url(to_):
            _ = upload(config, from_, to_, worker_count=worker_count, ignore_state=ignore_state, project_id=project_id,
                       source_path=source_path, disable_progress_bar=disable_progress_bar, duplicate_check=duplicate_check)
            output.update(_)
            if len(_.incomplete) > 0:
                raise ValueError("Not all transfers complete.")
        else:
            output.update(download(config, from_, to_))


@file_group.command(name="rm")
@click.pass_obj
def files_rm(config: Config):
    """Remove files from a project."""
    rm(config)
