from plastron.commands import BaseCommand
from plastron.exceptions import FailureException


def configure_cli(subparsers):
    parser_ping = subparsers.add_parser(
        name='ping',
        description='Check connection to the repository'
    )
    parser_ping.set_defaults(cmd_name='ping')


class Command(BaseCommand):
    def __call__(self, fcrepo, args):
        try:
            fcrepo.test_connection()
        except ConnectionError as e:
            raise FailureException(str(e)) from e
