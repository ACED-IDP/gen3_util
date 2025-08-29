"""Command line interface for calypr_dataframer."""

import sys
import tempfile
import pathlib
import click

from calypr_dataframer.dataframer import create_dataframe


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug mode')
@click.pass_context
def cli(ctx: click.Context, debug: bool):
    """Calypr Dataframer - Generate dataframes from FHIR metadata."""
    ctx.ensure_object(dict)
    ctx.obj['debug'] = debug


@cli.command("dataframe")
@click.argument('data_type',
                required=True,
                type=click.Choice(['Specimen', 'DocumentReference', 'ResearchSubject', 
                                 "MedicationAdministration", "GroupMember"]),
                default=None)
@click.argument("directory_path",
                type=click.Path(exists=True, file_okay=False),
                default="./META", required=False)
@click.argument("output_path",
                type=click.Path(file_okay=True), required=False)
@click.option('--dtale', 'launch_dtale', default=False, show_default=True, is_flag=True, 
              help='Open the dataframe in a browser using the dtale package for interactive data exploration.')
@click.option('--debug', is_flag=True)
@click.pass_context
def dataframe_cmd(ctx: click.Context, directory_path: str, output_path: str, 
                  launch_dtale: bool, data_type: str, debug: bool):
    """Generate a metadata dataframe from FHIR resources.

    \b
    DATA_TYPE: The type of FHIR resource to process
    DIRECTORY_PATH: The directory path to the metadata files (default: ./META)
    OUTPUT_PATH: The output path for the dataframe CSV file (default: {DATA_TYPE}.csv)
    """
    debug = debug or ctx.obj.get('debug', False)
    
    try:
        # Create a temporary directory for database operations
        with tempfile.TemporaryDirectory() as temp_dir:
            df = create_dataframe(directory_path, temp_dir, data_type)

            if launch_dtale:
                try:
                    import dtale
                    dtale.show(df, subprocess=False, open_browser=True, port=40000)
                except ImportError:
                    click.secho("dtale package not installed. Install with: pip install dtale", 
                               fg='red', err=True)
                    sys.exit(1)
            else:
                # Export to CSV
                file_name = output_path if output_path else f"{data_type}.csv"
                df.to_csv(file_name, index=False)
                click.secho(f"Saved {file_name}", fg='green', err=True)
                
    except Exception as e:
        click.secho(str(e), fg='red', err=True)
        if debug:
            raise
        sys.exit(1)


if __name__ == '__main__':
    cli()