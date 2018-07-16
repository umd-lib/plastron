DEFAULT_LOGGING_OPTIONS = {
        'version': 1,
        'formatters': {
            'full': {
                'format': '%(levelname)s|%(asctime)s|%(threadName)s|%(name)s|%(message)s'
                },
            'messageonly': {
                'format': '%(message)s'
                }
            },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'messageonly',
                'stream': 'ext://sys.stderr'
                },
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'full'
                }
            },
        'loggers': {
            '__main__': {
                'level': 'DEBUG',
                'handlers': ['console', 'file'],
                'propagate': False
                },
            'plastron': {
                'level': 'DEBUG',
                'handlers': ['console', 'file'],
                'propagate': False
                }
            },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console', 'file']
            }
        }
