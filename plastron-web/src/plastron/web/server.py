import click
from dotenv import load_dotenv
from waitress import serve

from plastron.web import create_app


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
    app = create_app(config_file)
    serve(app, listen=listen)


if __name__ == "__main__":
    run()
