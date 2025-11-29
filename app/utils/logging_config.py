from logging.config import dictConfig
import os

def setup_logging(log_file_path: str = "/app/logs/app.log"):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    dictConfig({
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
            }
        },

        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": "INFO"
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": log_file_path,
                "formatter": "default",
                "level": "INFO",
                "mode": "a"
            }
        },

        "loggers": {
            "": {  # root logger
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn.error": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
            "uvicorn.access": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
        }
    })
