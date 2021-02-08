from plastron.commands import BaseCommand


def test_override_repo_config():
    # Default implementation simply returns the provided dictionary
    command = BaseCommand()
    repo_config = {'foo': 'bar', 'quuz': 'baz'}
    assert repo_config is command.override_repo_config(repo_config)
