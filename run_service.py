#!/usr/bin/env python3
"""Container entrypoint: one process serves HTTP and NTP UDP.

The FastAPI lifespan starts the UDP responder and the sampler, so the web UI
and the NTP service always share the same in-memory clock state.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn

from app.config import load_settings
from app.web import create_app


def main() -> None:
    settings = load_settings()
    app = create_app(settings)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.http_port,
        log_level=os.environ.get("LOG_LEVEL", "info"),
        access_log=False,
    )


if __name__ == "__main__":
    main()
