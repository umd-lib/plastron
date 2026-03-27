from plastron.context import PlastronContext


def test_single_property_instance_returned():
    context = PlastronContext(
        {'REPOSITORY': {'REST_ENDPOINT': 'http://fcrepo-local:8080/fcrepo/rest'}}
    )
    assert context.endpoint is context.endpoint
    assert context.client is context.client
    assert context.repo is context.repo
