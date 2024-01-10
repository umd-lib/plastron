import logging
import time
from argparse import Namespace

from plastron.cli.commands import BaseCommand


logger = logging.getLogger(__name__)


def configure_cli(subparsers):
    parser = subparsers.add_parser(
        name='echo',
        description=(
            'Diagnostic command for echoing input to output. '
            'Primarily intended for testing synchronous message processing.'
        )
    )
    parser.add_argument(
        '-e', '--echo-delay',
        help='The amount of time to delay the reply, in seconds',
        required=False,
        action='store'
    )
    parser.add_argument(
        '-b', '--body',
        help='The text to echo back',
        required=True,
        action='store'
    )
    parser.set_defaults(cmd_name='echo')


class Command(BaseCommand):
    def __call__(self, *args, **kwargs):
        self.execute(*args, **kwargs)
        print(self.result)

    @staticmethod
    def parse_message(message):
        message_body = message.body.encode('utf-8').decode('utf-8-sig')
        echo_delay = message.headers.get('echo-delay', "0")

        return Namespace(
            body=message_body,
            echo_delay=echo_delay
        )

    def execute(self, _repo, args):
        if args.echo_delay:
            time.sleep(int(args.echo_delay))

        self.result = args.body
