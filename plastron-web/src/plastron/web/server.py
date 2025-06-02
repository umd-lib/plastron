import logging

import click
from dotenv import load_dotenv
from waitress import serve

from plastron.web import create_app, __version__

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--listen',
    default='0.0.0.0:5000',
    help='Address and port to listen on. Default is "0.0.0.0:5000".',
    metavar='[ADDRESS]:PORT',
)
@click.option(
    '-c', '--config-file',
    type=click.Path(exists=True),
    help='Configuration file',
    required=True,
)
def run(listen: bool, config_file: str):
    load_dotenv()
    server_identity = f'plastrond-http/{__version__}'
    logger.info(f'Starting {server_identity}')
    try:
        serve(
            app=create_app(config_file),
            listen=listen,
            ident=server_identity,
        )
    except (OSError, RuntimeError) as e:
        logger.error(f'Exiting: {e}')
        raise SystemExit(1) from e


if __name__ == "__main__":
    run()
