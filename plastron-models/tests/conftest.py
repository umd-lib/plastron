def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line(
        "markers", "issue(key,url): mark test as originating from a particular issue"
    )
