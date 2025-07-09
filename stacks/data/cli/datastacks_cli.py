"""Datastacks CLI Module.

This module provides command-line interfaces to generate and manage data workloads within the Stacks Data Platform.
"""
import logging

from stacks.data.logger import setup_logger
from stacks.data.platforms.azure.constants import CONFIG_CONTAINER_NAME
from stacks.data.pyspark.data_quality.main import data_quality_main
from stacks.data.generate.data_workloads import validate_yaml_config, generate_pipeline
from stacks.data.generate.template_config import IngestWorkloadConfigModel, ProcessingWorkloadConfigModel

try:
    import click
    from click_loglevel import LogLevel
except ImportError:
    raise ImportError(
        "Required dependencies for Datastacks CLI are not installed. "
        "Please install them using: pip install stacks-data[cli]"
    )


@click.group()
@click.help_option("-h", "--help")
@click.option("--log-level", "-l", type=LogLevel(), default=logging.INFO)
def cli(log_level):
    """Specify log level for Datastacks."""
    setup_logger("datastacks", log_level)


@cli.group()
@click.help_option("-h", "--help")
def generate():
    """Generate new data workload."""
    pass


@generate.command()
@click.help_option("-h", "--help")
@click.option("--config", "-c", type=str, help="Absolute path to config file on local machine")
@click.option(
    "--data-quality/--no-data-quality",
    "-dq/-ndq",
    default=False,
    help="Flag to determine whether to include data quality in template",
)
def ingest(config, data_quality):
    """Generate new data ingest workload."""
    validated_config = validate_yaml_config(config, IngestWorkloadConfigModel)
    generate_pipeline(validated_config, data_quality)


@generate.command()
@click.help_option("-h", "--help")
@click.option("--config", "-c", type=str, help="Absolute path to config file on local machine")
@click.option(
    "--data-quality/--no-data-quality",
    "-dq/-ndq",
    default=False,
    help="Flag to determine whether to include data quality in template",
)
def processing(config, data_quality):
    """Generate new data processing example workload."""
    validated_config = validate_yaml_config(config, ProcessingWorkloadConfigModel)
    generate_pipeline(validated_config, data_quality)


@cli.command()
@click.help_option("-h", "--help")
@click.option("--config-path", help="Path to a JSON config inside an Azure Blob container.")
@click.option(
    "--container",
    default=CONFIG_CONTAINER_NAME,
    show_default=True,
    help="Name of the container for storing configurations.",
)
def dq(config_path, container):
    """Perform data quality check."""
    data_quality_main(config_path, container)


if __name__ == "__main__":
    cli()
