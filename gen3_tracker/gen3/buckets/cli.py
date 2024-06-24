import click

from gen3_util.buckets.lister import ls
from gen3_util.repo import NaturalOrderGroup, CLIOutput
from gen3_util.config import Config, ensure_auth


@click.group(name='buckets', cls=NaturalOrderGroup, invoke_without_command=True)
@click.pass_context
def bucket_group(ctx: click.Context):
    """Project buckets."""
    if ctx.invoked_subcommand is None:
        ctx.invoke(ls_command)


@bucket_group.command(name="ls")
@click.pass_obj
def ls_command(config: Config):
    """List buckets managed by commons."""
    with CLIOutput(config=config) as output:
        try:
            auth = ensure_auth(config=config, validate=True)
            assert auth, "auth required"
            output.update({'endpoint': auth.endpoint})
            output.update(ls(config))
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
