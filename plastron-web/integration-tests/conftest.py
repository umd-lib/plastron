import pytest


@pytest.fixture
def inbox_url():
    return 'http://localhost:5000/inbox'


@pytest.fixture
def jsonld_context():
    return [
        "https://www.w3.org/ns/activitystreams",
        {
            "umdact": "http://vocab.lib.umd.edu/activity#",
            "Publish": "umdact:Publish",
            "PublishHidden": "umdact:PublishHidden",
            "Unpublish": "umdact:Unpublish"
        }
    ]
