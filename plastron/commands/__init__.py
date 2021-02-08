class BaseCommand:
    def __init__(self, config=None):
        if config is None:
            config = {}
        self.config = config

    # Enable repository config dictionary to be overridden by the command before
    # actually creating the respository.
    #
    # The default implemented of this method simply returns the provided
    # repo_config dictionary without change
    def override_repo_config(self, repo_config, args={}):
        return repo_config
