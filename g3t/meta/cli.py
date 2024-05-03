
import click
import pathlib
import sys

from halo import Halo
from g3t import Config, ENV_VARIABLE_PREFIX
from g3t.common import INFO_COLOR, ERROR_COLOR


@click.group()
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_context
def meta(ctx, project_id):
    """Manage the META directory."""
    pass


@meta.command()
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.pass_context
def init(ctx, project_id):
    """Initialize the META directory based on the MANIFEST."""
    try:
        from g3t.common import INFO_COLOR, ERROR_COLOR
        from g3t.meta.skeleton import update_meta_files
        from g3t.meta.validator import validate as validate_dir

        with Halo(text='Generating', spinner='line', placement='right', color='white'):
            config: Config = ctx.obj
            if not project_id:
                project_id = config.gen3.project_id
            updated_files = update_meta_files(config.dry_run, project_id)
        click.secho(f"Updated {len(updated_files)} metadata files.", fg=INFO_COLOR, file=sys.stderr)
        result = validate_dir('META')
        click.secho(result, fg=INFO_COLOR, file=sys.stderr)

    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if ctx.obj.debug:
            raise


@meta.command()
@click.argument('directory', type=click.Path(exists=True), default='META')
@click.pass_obj
def validate(ctx, directory):
    """Validate FHIR data"""
    try:
        from g3t.meta.validator import validate as validate_dir

        result = validate_dir(directory)
        click.secho(result, fg=INFO_COLOR, file=sys.stderr)
    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        raise
        # if ctx.obj.debug:
        #     raise


@meta.command("graph")
@click.argument("directory_path",
                type=click.Path(exists=True, file_okay=False),
                default="META", required=False)
@click.argument("output_path",
                type=click.Path(file_okay=True),
                default="meta.html", required=False)
@click.option('--browser', default=False, show_default=True, is_flag=True, help='Open the graph in a browser.')
@click.pass_obj
def render_graph(config: Config, directory_path: str, output_path: str, browser: bool):
    """Render metadata as a network graph.

    \b
    directory_path: The directory path to the metadata. default [META]
    output_path: The output path for the network graph. default [meta.html]
    """
    try:
        from g3t.meta.visualizer import create_network_graph
        import webbrowser

        assert pathlib.Path(directory_path).exists(), f"Directory {directory_path} does not exist."
        with Halo(text='Graphing', spinner='line', placement='right', color='white'):
            output_path = pathlib.Path(output_path)
            create_network_graph(directory_path, output_path)
            url = f"file://{output_path.absolute()}"
        click.secho(f"Saved {output_path}, open it in your browser to view the network.", fg=INFO_COLOR, file=sys.stderr)
        if browser:
            webbrowser.open(url)
    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise


@meta.command("dataframe")
@click.argument("directory_path",
                type=click.Path(exists=True, file_okay=False),
                default="./META", required=False)
@click.argument("output_path",
                type=click.Path(file_okay=True),
                default="meta.csv", required=False)
@click.option('--dtale', 'launch_dtale', default=False, show_default=True, is_flag=True, help='Open the graph in a browser using the dtale package for interactive data exploration.')
@click.pass_obj
def render_df(config: Config, directory_path: str, output_path: str, launch_dtale: bool):
    """Render a metadata dataframe.

    \b
    directory_path: The directory path to the metadata.
    output_path: The output path for the dataframe. default [meta.csv]
    """
    try:
        from g3t.meta.dataframer import create_dataframe
        with Halo(text='Creating DataFrame', spinner='line', placement='right', color='white'):
            df = create_dataframe(directory_path, config.work_dir)

        if launch_dtale:
            from dtale import dtale
            dtale.show(df, subprocess=False, open_browser=True, port=40000)
        else:
            # export to csv
            df.to_csv(output_path, index=False)
            click.secho(f"Saved {output_path}", fg=INFO_COLOR, file=sys.stderr)
    except Exception as e:
        click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
        if config.debug:
            raise
