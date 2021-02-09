class BaseCommand:
    def __init__(self, config=None):
        if config is None:
            config = {}
        self.config = config

    def repo_config(self, repo_config, args={}):
        """
        Enable default repository config dictionary to be overridden by the
        command before actually creating the repository.

        The default implemention of this method simply returns the provided
        repo_config dictionary without change
        """
        return repo_config
