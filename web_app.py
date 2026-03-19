from __future__ import annotations

import os

os.environ.setdefault("EMBED_RUNTIME_WORKER", "0")

from app import app  # noqa: E402
