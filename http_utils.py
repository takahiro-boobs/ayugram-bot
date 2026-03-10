import logging
import random
import time
from typing import Iterable, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_RETRY_STATUSES = {408, 429, 500, 502, 503, 504}


def request_with_retry(
    method: str,
    url: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 25,
    max_attempts: int = 3,
    backoff_seconds: float = 0.7,
    backoff_factor: float = 2.0,
    jitter: float = 0.2,
    retry_statuses: Optional[Iterable[int]] = None,
    allow_retry: bool = False,
    log_context: str = "",
    **kwargs,
) -> requests.Response:
    attempts = max(1, int(max_attempts or 1))
    if not allow_retry:
        attempts = 1
    statuses = set(retry_statuses or DEFAULT_RETRY_STATUSES)

    def _do_request() -> requests.Response:
        if session is not None:
            return session.request(method, url, timeout=timeout, **kwargs)
        return requests.request(method, url, timeout=timeout, **kwargs)

    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            response = _do_request()
            if allow_retry and response.status_code in statuses and attempt < attempts:
                response.close()
                delay = backoff_seconds * (backoff_factor ** (attempt - 1))
                if jitter:
                    delay *= 1 + (random.random() * jitter)
                if log_context:
                    logger.warning("retry_http_status: ctx=%s status=%s attempt=%s delay=%.2fs", log_context, response.status_code, attempt, delay)
                time.sleep(delay)
                continue
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException) as exc:
            last_error = exc
            if not allow_retry or attempt >= attempts:
                raise
            delay = backoff_seconds * (backoff_factor ** (attempt - 1))
            if jitter:
                delay *= 1 + (random.random() * jitter)
            if log_context:
                logger.warning("retry_http_error: ctx=%s error=%s attempt=%s delay=%.2fs", log_context, exc, attempt, delay)
            time.sleep(delay)
    if last_error is not None:
        raise last_error
    raise RuntimeError("request_with_retry failed without response")
