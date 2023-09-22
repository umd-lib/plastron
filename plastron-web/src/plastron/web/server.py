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
def run(listen):
    load_dotenv()
    app = create_app()
    serve(app, listen=listen)
