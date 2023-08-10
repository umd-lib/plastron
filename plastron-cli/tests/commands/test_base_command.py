from plastron.cli.commands import BaseCommand


def test_repo_config():
    # Default implementation simply returns the provided dictionary
    command = BaseCommand()
    repo_config = {'foo': 'bar', 'quuz': 'baz'}
    assert repo_config is command.repo_config(repo_config)
