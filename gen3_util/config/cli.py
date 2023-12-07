import subprocess

import click

from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.config import ensure_auth
from gen3_util.config.config import Config


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
        # cast state dir to string, so it prints out nicely
        _ = config.dict()
        _['state_dir'] = str(config.state_dir)
        _['gen3_client_version'] = _gen3_client_version()
        output.update(_)
        # print(_access_token_info(config))


def _gen3_client_version() -> str:
    """Get the version of gen3-client."""
    try:
        results = subprocess.run("gen3-client --version".split(), capture_output=True)
        return results.stdout.decode('utf-8').strip().split()[-1]
    except FileNotFoundError:
        return "ERROR gen3-client not installed"


def _access_token_info(config: Config) -> dict:
    """Get the access token info."""
    auth = ensure_auth(profile=config.gen3.profile)
    _ = {'endpoint': auth.endpoint, 'access_token': auth.get_access_token()}

    return _
