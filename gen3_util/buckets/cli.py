import click

from gen3_util.buckets.lister import ls
from gen3_util.repo import NaturalOrderGroup, CLIOutput
from gen3_util.config import Config, ensure_auth


@click.group(name='buckets', cls=NaturalOrderGroup)
@click.pass_obj
def bucket_group(config: Config):
    """Project buckets."""
    pass


@bucket_group.command(name="ls")
@click.pass_obj
def ls_command(config: Config):
    """List buckets managed by commons."""
    with CLIOutput(config=config) as output:
        auth = ensure_auth(profile=config.gen3.profile, validate=True)
        output.update({'endpoint': auth.endpoint})
        output.update(ls(config))
