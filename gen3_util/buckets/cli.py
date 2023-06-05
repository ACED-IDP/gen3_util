import click

from gen3_util.buckets.lister import ls
from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.config import Config, ensure_auth


@click.group(name='buckets', cls=NaturalOrderGroup)
@click.pass_obj
def bucket_group(config: Config):
    """Manage Gen3 buckets."""
    pass


@bucket_group.command(name="ls")
@click.pass_obj
def ls_command(config: Config):
    """Test connectivity to Gen3 endpoint."""
    with CLIOutput(config=config) as output:
        auth = ensure_auth(config.gen3.refresh_file, validate=True)
        output.update({'endpoint': auth.endpoint})
        output.update(ls(config))
