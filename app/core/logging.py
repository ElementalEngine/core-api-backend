from __future__ import annotations

import logging


def configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
