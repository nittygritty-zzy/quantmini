#!/usr/bin/env python3
"""
 QuantMini CLI - Main entry point.
"""

import click
import logging
import sys
from pathlib import Path

from .commands import data, pipeline, config_cmd, validate, schema, api, polygon


# Configure logging to show INFO and above to stderr with simple format
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    stream=sys.stderr,
    force=True
)


@click.group()
@click.version_option(version='0.2.0', prog_name='quantmini')
@click.pass_context
def cli(ctx):
    """
    QuantMini - High-Performance Data Pipeline for Financial Market Data.

    A production-ready pipeline for processing Polygon.io data with Qlib integration.
    """
    ctx.ensure_object(dict)


# Register command groups
cli.add_command(data.data)
cli.add_command(pipeline.pipeline)
cli.add_command(config_cmd.config)
cli.add_command(validate.validate)
cli.add_command(schema.schema)
cli.add_command(api.api)
cli.add_command(polygon.polygon)


if __name__ == '__main__':
    cli()
