from time import sleep
from uuid import uuid4

import pytest
from flask import url_for
from http_server_mock import HttpServerMock

from plastron.exceptions import FailureException
from plastron.http import Repository, Transaction


@pytest.fixture
def repo_app():
    app = HttpServerMock(__name__)

    @app.route('/')
    def root():
        return 'Mock fcrepo server', 200

    @app.route('/fcr:tx', methods=['POST'])
    def create_txn():
        txn_id = uuid4()
        return '', 201, {'Location': url_for('txn', _external=True, txn_id=txn_id)}

    @app.route('/tx:<txn_id>', methods=['GET'])
    def txn(txn_id):
        return txn_id, 200

    @app.route('/tx:<txn_id>/fcr:tx', methods=['POST'])
    def maintain_txn(txn_id):
        """
        This mock repository server always fails when a client tries to
        request an extension to the transaction expiration

        :param txn_id:
        :return:
        """
        return f'{txn_id} is no longer alive', 400

    return app


@pytest.fixture
def repo_base_config():
    """Required parameters for Repository configuration"""
    return {
        'REST_ENDPOINT': 'http://localhost:9999',
        'RELPATH': '/pcdm',
        'LOG_DIR': '/logs',
        'AUTH_TOKEN': 'foobar'
    }


def test_failure_to_maintain_txn(repo_base_config, repo_app):
    with repo_app.run('localhost', 9999):
        repo = Repository(repo_base_config)
        # send the keep-alive ping once per second
        with Transaction(repo, keep_alive=1):
            # wait 2 seconds to be sure there has been a keep-alive ping
            sleep(2)
            # now we expect further requests to fail, because the transaction
            # keep-alive has failed
            with pytest.raises(FailureException):
                repo.get('http://localhost:9999/')
