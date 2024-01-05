import subprocess

import click

from gen3_util import Config
from gen3_util.cli import NaturalOrderGroup, CLIOutput


@click.group(name='config', cls=NaturalOrderGroup)
@click.pass_obj
def config_group(config):
    """Configure this utility."""
    pass


@config_group.command(name="ls")
@click.pass_obj
def config_ls(config: Config):
    """Show defaults."""
    with CLIOutput(config) as output:
        # decorate config with some extra info
        config.gen3.version = _gen3_client_version()
        output.update({'config': config.model_dump()})


def _gen3_client_version() -> str:
    """Get the version of gen3-client."""
    try:
        results = subprocess.run("gen3-client --version".split(), capture_output=True)
        return results.stdout.decode('utf-8').strip().split()[-1]
    except FileNotFoundError:
        return "ERROR gen3-client not installed"
