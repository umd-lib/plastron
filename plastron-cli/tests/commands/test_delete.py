import argparse
from contextlib import nullcontext as does_not_raise
from pathlib import Path

import httpretty
import pytest
from httpretty import HEAD, GET, DELETE

from plastron.cli.commands.delete import Command
from plastron.repo import RepositoryError
from plastron.jobs import ItemLog


def register_responses(responses, uri):
    for method, kwargs in responses.items():
        httpretty.register_uri(method=method, uri=uri, **kwargs)


@pytest.mark.parametrize(
    ('path', 'responses', 'expectation'),
    [
        # resource exists, delete successful
        (
            'ok',
            {
                HEAD: {'status': 200},
                GET: {'status': 200, 'adding_headers': {'Content-Type': 'application/n-triples'}, 'body': ''},
                DELETE: {'status': 204},
            },
            does_not_raise(),
        ),
        # resource not found
        (
            'not_found',
            {
                HEAD: {'status': 404},
                GET: {'status': 404, 'body': ''},
                DELETE: {'status': 404},
            },
            does_not_raise(),
        ),
        # resource gone
        (
            'gone',
            {
                HEAD: {'status': 410},
                GET: {'status': 410, 'body': ''},
                DELETE: {'status': 410},
            },
            does_not_raise(),
        ),
        # resource exists, internal server error while deleting
        (
            'internal_server_error',
            {
                HEAD: {'status': 200},
                GET: {'status': 200, 'adding_headers': {'Content-Type': 'application/n-triples'}, 'body': ''},
                DELETE: {'status': 500},
            },
            pytest.raises(RepositoryError),
        ),
        # resource exists, bad request error while deleting
        (
            'bad_request',
            {
                HEAD: {'status': 200},
                GET: {'status': 200, 'adding_headers': {'Content-Type': 'application/n-triples'}, 'body': ''},
                DELETE: {'status': 400},
            },
            pytest.raises(RepositoryError),
        ),
    ]
)
@httpretty.activate
def test_delete_command(plastron_context, register_transaction, path, responses, expectation):
    txn_url = register_transaction()
    uri = txn_url.add_path_segment(path)
    register_responses(
        uri=uri,
        responses=responses,
    )
    args = argparse.Namespace(
        delegated_user=None,
        completed=None,
        dry_run=False,
        uris=[f'/{path}'],
        use_transactions=True,
        recursive=None,
    )
    plastron_context.args = args

    cmd = Command(context=plastron_context)
    with expectation:
        cmd(args)


@httpretty.activate
def test_completed_log(datadir, repo, plastron_context, register_transaction):
    txn_url = register_transaction()
    url = txn_url.add_path_segment('test')
    deleted_url = str(repo['/test'].url)
    title_triple = f'<{deleted_url}> <http://purl.org/dc/terms/title> "Test Resource" .'
    register_responses(
        uri=url,
        responses={
            HEAD: {'status': 200},
            GET: {'status': 200, 'adding_headers': {'Content-Type': 'application/n-triples'}, 'body': title_triple},
            DELETE: {'status': 204},
        },
    )
    completed_log_path: Path = datadir / 'completed.csv'
    args = argparse.Namespace(
        delegated_user=None,
        completed=completed_log_path,
        dry_run=False,
        uris=['/test'],
        use_transactions=True,
        recursive=None,
    )
    plastron_context.args = args

    cmd = Command(context=plastron_context)
    cmd(args)

    assert completed_log_path.exists()
    completed_log = ItemLog(filename=completed_log_path, fieldnames=['uri', 'title', 'timestamp'], keyfield='uri')
    assert deleted_url in completed_log
    assert completed_log[0]['title'] == 'Test Resource'


# TODO: test dry-run
# TODO: test recursive
