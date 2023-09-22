import pytest

from plastron.client import RepositoryStructure
from plastron.stomp.commands import importcommand
from plastron.stomp.listeners import MessageProcessor
from plastron.stomp.messages import PlastronCommandMessage

cmd = importcommand.Command()

# "Flat" layout config
flat_repo_config = {
    'REST_ENDPOINT': 'http://example.com/rest',
    'RELPATH': '/pcdm',
    'LOG_DIR': 'logs',
    'STRUCTURE': 'flat'
}

# "Hierarchical" layout config
hierarchical_repo_config = {
    'REST_ENDPOINT': 'http://example.com/rest',
    'RELPATH': '/dc/2021/2',
    'LOG_DIR': 'logs',
    'STRUCTURE': 'hierarchical'
}

# Import message does not specify structure
no_structure_message = PlastronCommandMessage(
    message_id='TEST-no-structure',
    job_id='1',
    command='import'
)

# Import message specifies "flat" structure
flat_structure_message = PlastronCommandMessage(
    message_id='TEST-flat-structure',
    job_id='1',
    command='import',
    args={'structure': 'flat'}
)

# Import message specified "hierarchical" structure
hierarchical_structure_message = PlastronCommandMessage(
    message_id='TEST-hierarchical-structure',
    job_id='1',
    command='import',
    args={'structure': 'hierarchical'}
)


@pytest.mark.parametrize(
    ('repo_config', 'message', 'expected_structure'),
    [
        (flat_repo_config, no_structure_message, RepositoryStructure.FLAT),
        (hierarchical_repo_config, no_structure_message, RepositoryStructure.HIERARCHICAL),
        (flat_repo_config, hierarchical_structure_message, RepositoryStructure.HIERARCHICAL),
        (hierarchical_repo_config, flat_structure_message, RepositoryStructure.FLAT)
    ]
)
def test_configure_repo_structure(repo_config, message, expected_structure):
    # message structure should override config structure
    processor = MessageProcessor(command_config={}, repo_config=repo_config)
    repo = processor.configure_repo(message)
    assert repo.client.structure == expected_structure


# "relpath" layout config
relpath_repo_config = {
    'REST_ENDPOINT': 'http://example.com/rest',
    'RELPATH': '/pcdm',
    'LOG_DIR': 'logs',
    'STRUCTURE': 'flat'
}

# Import message without relpath
no_relpath_message = PlastronCommandMessage(
    message_id='TEST-without-relpath',
    job_id='1',
    command='import',
    args={'structure': 'flat'},
)

relpath_message = PlastronCommandMessage(
    message_id='TEST-with-relpath',
    job_id='1',
    command='import',
    args={
        'structure': 'flat',
        'relpath': '/test-relpath'
    },
)


@pytest.mark.parametrize(
    ('repo_config', 'message', 'expected_relpath'),
    [
        (relpath_repo_config, no_relpath_message, relpath_repo_config['RELPATH']),
        (relpath_repo_config, relpath_message, relpath_message.args['relpath']),
    ]
)
def test_configure_repo_relpath(repo_config, message, expected_relpath):
    # message structure should override config structure
    processor = MessageProcessor(command_config={}, repo_config=repo_config)
    repo = processor.configure_repo(message)
    assert repo.endpoint.relpath == expected_relpath
