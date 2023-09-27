from pathlib import Path
from unittest.mock import MagicMock

from plastron.client import Client, paths_to_create


def test_paths_to_create():
    mock_client = MagicMock(spec=Client)
    mock_client.path_exists.return_value = True

    assert paths_to_create(mock_client, Path('/foo')) == []
