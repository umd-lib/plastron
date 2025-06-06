import json
from unittest.mock import MagicMock, ANY

import pytest

from plastron.context import PlastronContext
from plastron.handles import HandleServiceClient, HandleInfo
from plastron.repo import Repository
from plastron.repo.publish import PublishableResource


@pytest.fixture
def post_data():
    return {
        "@context": [
            "https://www.w3.org/ns/activitystreams",
            {
                "umdact": "http://vocab.lib.umd.edu/activity#",
                "Publish": "umdact:Publish",
                "PublishHidden": "umdact:PublishHidden",
                "Unpublish": "umdact:Unpublish"
            }
        ],
        "type": "Publish",
        "object": ["http://fcrepo-local:8080/fcrepo/rest/test/obj"]
    }


@pytest.fixture
def request_headers():
    mimetype = 'application/json'
    return {
        'Content-Type': mimetype,
        'Accept': mimetype
    }


@pytest.fixture
def mock_resource():
    mock_resource = MagicMock(spec=PublishableResource)
    mock_resource.read.return_value = mock_resource
    mock_handle = MagicMock(spec=HandleInfo)
    mock_resource.publish.return_value = mock_handle
    return mock_resource


@pytest.fixture
def mock_context(mock_resource):
    mock_repo = MagicMock(spec=Repository)
    mock_repo.__getitem__.return_value = mock_resource
    mock_context = MagicMock(spec=PlastronContext, repo=mock_repo, handle_client=MagicMock(spec=HandleServiceClient))
    mock_context.get_public_url.return_value = 'http://digital-local/foo'
    return mock_context


@pytest.mark.parametrize(
    ('input_json', 'expected_args'),
    [
        (
            # json input
            {
                "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    {
                        "umdact": "http://vocab.lib.umd.edu/activity#",
                        "Publish": "umdact:Publish",
                        "PublishHidden": "umdact:PublishHidden",
                        "Unpublish": "umdact:Unpublish"
                    }
                ],
                "type": "Publish",
                "object": ["http://fcrepo-local:8080/fcrepo/rest/test/obj"]
            },
            # expected args
            {
                'force_hidden': False,
                'force_visible': False
            },
        ),
        (
            # json input
            {
                "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    {
                        "umdact": "http://vocab.lib.umd.edu/activity#",
                        "Publish": "umdact:Publish",
                        "PublishHidden": "umdact:PublishHidden",
                        "Unpublish": "umdact:Unpublish"
                    }
                ],
                "type": "PublishHidden",
                "object": ["http://fcrepo-local:8080/fcrepo/rest/test/obj"]
            },
            # expected args
            {
                'force_hidden': True,
                'force_visible': False
            },
        ),
    ],
)
def test_new_activity_publish(
        app,
        app_client,
        mock_context,
        mock_resource,
        post_data,
        request_headers,
        input_json,
        expected_args,
):
    app.config['CONTEXT'] = mock_context

    url = '/inbox'
    response = app_client.post(url, data=json.dumps(input_json), headers=request_headers)
    assert response.status_code == 201

    mock_resource.publish.assert_called_once_with(handle_client=ANY, public_url=ANY, **expected_args)


@pytest.mark.parametrize(
    ('input_json', 'expected_args'),
    [
        (
            # json input
            {
                "@context": [
                    "https://www.w3.org/ns/activitystreams",
                    {
                        "umdact": "http://vocab.lib.umd.edu/activity#",
                        "Publish": "umdact:Publish",
                        "PublishHidden": "umdact:PublishHidden",
                        "Unpublish": "umdact:Unpublish"
                    }
                ],
                "type": "Unpublish",
                "object": ["http://fcrepo-local:8080/fcrepo/rest/test/obj"]
            },
            # expected args
            {
                'force_hidden': False,
                'force_visible': False
            },
        )
    ],
)
def test_new_activity_unpublish(
        app,
        app_client,
        mock_context,
        mock_resource,
        post_data,
        request_headers,
        input_json,
        expected_args,
):
    app.config['CONTEXT'] = mock_context

    url = '/inbox'
    response = app_client.post(url, data=json.dumps(input_json), headers=request_headers)
    assert response.status_code == 201

    mock_resource.unpublish.assert_called_once_with(**expected_args)


@pytest.mark.parametrize(
    'input_json',
    [
        # Missing type
        {
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                {
                    "umdact": "http://vocab.lib.umd.edu/activity#",
                    "Publish": "umdact:Publish",
                    "PublishHidden": "umdact:PublishHidden",
                    "Unpublish": "umdact:Unpublish"
                }
            ],
            "object": ["http://fcrepo-local:8080/fcrepo/rest/test/obj"]
        },
        # Invalid type
        {
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                {
                    "umdact": "http://vocab.lib.umd.edu/activity#",
                    "Publish": "umdact:Publish",
                    "PublishHidden": "umdact:PublishHidden",
                    "Unpublish": "umdact:Unpublish"
                }
            ],
            "type": "foo",
            "object": ["http://fcrepo-local:8080/fcrepo/rest/test/obj"]
        },
        # Missing target object
        {
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                {
                    "umdact": "http://vocab.lib.umd.edu/activity#",
                    "Publish": "umdact:Publish",
                    "PublishHidden": "umdact:PublishHidden",
                    "Unpublish": "umdact:Unpublish"
                }
            ],
            "type": "Publish",
        }
    ],
)
def test_new_activity_invalid_input(app_client, post_data, request_headers, input_json):
    url = '/inbox'
    response = app_client.post(url, data=json.dumps(input_json), headers=request_headers)
    assert response.status_code == 400
