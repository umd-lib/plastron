class ConfigError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class DataReadException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class RESTAPIException(Exception):
    def __init__(self, response):
        self.response = response

    def __str__(self):
        return '{0} {1}'.format(self.response.status_code, self.response.reason)


class FailureException(Exception):
    pass


class NoValidationRulesetException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class BinarySourceError(Exception):
    pass


class BinarySourceNotFoundError(BinarySourceError):
    pass
