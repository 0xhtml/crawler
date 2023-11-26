"""A crawler."""

import logging
from typing import Union

Logger = Union[logging.Logger, logging.LoggerAdapter]

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter(
        "%(levelname)s - %(url)s - %(message)s",
        defaults={"url": "ROOT"},
    ),
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
