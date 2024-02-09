import pytest

import json
from conftest import config_file_path
from unittest.mock import MagicMock, patch, ANY

from plastron.web import create_app


# from plastron.web.activitystream

@pytest.fixture
def app_client(request):
    app = create_app(config_file_path(request))
    return app.test_client()


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
                    'uris': ["http://fcrepo-local:8080/fcrepo/rest/test/obj"],
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
                    'uris': ["http://fcrepo-local:8080/fcrepo/rest/test/obj"],
                    'force_hidden': True,
                    'force_visible': False
                },
        ),
    ],
)
@patch("plastron.web.activitystream.get_command")
def test_new_activity_publish(get_command_mock, app_client, post_data, request_headers, input_json, expected_args):
    mock_command = MagicMock()
    get_command_mock.return_value = mock_command

    url = '/inbox'
    response = app_client.post(url, data=json.dumps(input_json), headers=request_headers)
    assert response.status_code == 201

    get_command_mock.assert_called()
    activity_obj = get_command_mock.call_args[0][0]
    assert activity_obj.publish is True
    assert activity_obj.unpublish is False
    mock_command.assert_called_once_with(ANY, **expected_args)


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
                'uris': ["http://fcrepo-local:8080/fcrepo/rest/test/obj"],
                'force_hidden': False,
                'force_visible': False
            },
        )
    ],
)
@patch("plastron.web.activitystream.get_command")
def test_new_activity_unpublish(get_command_mock, app_client, post_data, request_headers, input_json, expected_args):
    mock_command = MagicMock()
    get_command_mock.return_value = mock_command

    url = '/inbox'
    response = app_client.post(url, data=json.dumps(input_json), headers=request_headers)
    assert response.status_code == 201

    get_command_mock.assert_called_once()
    activity_obj = get_command_mock.call_args[0][0]
    assert activity_obj.publish is False
    assert activity_obj.unpublish is True
    mock_command.assert_called_once_with(ANY, **expected_args)


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
@patch("plastron.web.activitystream.get_command")
def test_new_activity_invalid_input(get_command_mock, app_client, post_data, request_headers, input_json):
    mock_command = MagicMock()
    get_command_mock.return_value = mock_command

    url = '/inbox'
    response = app_client.post(url, data=json.dumps(input_json), headers=request_headers)
    assert response.status_code == 400

    get_command_mock.assert_not_called()
