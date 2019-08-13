#!/usr/bin/env python3
import argparse
import signal
import sys
import yaml
from stomp import ConnectionListener, Connection
from threading import Thread
from plastron import version
from plastron.http import Repository

class Exporter:
    def __init__(self, broker, completed_queue, repository):
        self.broker = broker
        self.completed_queue = completed_queue
        self.repository = repository

    def __call__(self, job_id=None, uris=None):
        if job_id is not None and uris is not None:
            for uri in uris:
                print(f'GET {uri}')
                r = self.repository.head(uri)
                print(f'--> {r.status_code}')

            # TODO: determine conditions for success or failure of the job
            self.broker.send(f'/queue/{self.completed_queue}', '', headers={
                'ArchelonExportJobId': job_id,
                'ArchelonExportJobStatus': 'Ready',
                'persistent': 'true'
            })

class Listener(ConnectionListener):
    def __init__(self, broker, completed_queue, repository):
        self.exporter = Exporter(broker, completed_queue, repository)

    def on_message(self, headers, body):
        print(headers)
        kwargs = {
            'job_id': headers['ArchelonExportJobId'],
            'uris': body.split('\n'),
        }
        # spawn a new thread to handle this message
        Thread(target=self.exporter, kwargs=kwargs).start()

def main():
    parser = argparse.ArgumentParser(
        prog='plastron',
        description='Batch operations daemon for Fedora 4.'
    )
    parser.add_argument(
        '-c', '--config',
        help = 'Path to configuration file.',
        action = 'store'
    )

    # parse command line args
    args = parser.parse_args()

    with open(args.config, 'r') as config_file:
        config = yaml.safe_load(config_file)

    repo = Repository(config['REPOSITORY'], ua_string=f'plastron/{version}')
    broker_config = config['MESSAGE_BROKER']

    message_broker = tuple(broker_config['SERVER'].split(':', 2))
    conn = Connection([message_broker])
    conn.set_listener('', Listener(conn, broker_config['EXPORT_JOBS_COMPLETED_QUEUE'], repo))
    conn.start()
    conn.connect()
    conn.subscribe(destination=f"/queue/{broker_config['EXPORT_JOBS_QUEUE']}", id='plastron')

    try:
        while True:
            signal.pause()
    except KeyboardInterrupt:
        sys.exit()

if __name__ == "__main__":
    main()
