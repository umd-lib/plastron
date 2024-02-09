import os


def config_file_path(request):
    test_dir = os.path.dirname(request.module.__file__)
    return os.path.join(test_dir, "configs/plastron-config.yml")
