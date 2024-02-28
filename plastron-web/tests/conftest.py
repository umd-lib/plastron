import os

import pytest


@pytest.fixture
def config_file_path():
    def _config_file_path(request):
        test_dir = os.path.dirname(request.module.__file__)
        return os.path.join(test_dir, "configs/plastron-config.yml")
    return _config_file_path
