DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s",
            "datefmt": DATE_FORMAT,
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "vgarus": {
            "formatter": "verbose",
            "class": "logging.FileHandler",
            "filename": "vgarus.log",
        },
    },
    "loggers": {
        "rii.vgarus.client": {
            "handlers": ["vgarus"],
            "level": "INFO",
            "propagate": False,
        },
        "tornado.access": {
            "level": "WARNING",
        },
    },
    "root": {
        "level": "INFO",
        "formatter": "verbose",
        "handlers": ["console"],
    },
}
