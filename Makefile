PYTHON ?= python3

.PHONY: install-dev test test-unittest run-web run-runtime-worker run-helper

install-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt

test:
	$(PYTHON) -m pytest -q

test-unittest:
	$(PYTHON) -m unittest discover -s tests -q

run-web:
	EMBED_RUNTIME_WORKER=0 $(PYTHON) -m uvicorn web_app:app --reload

run-runtime-worker:
	$(PYTHON) runtime_worker.py

run-helper:
	PUBLISH_RUNNER_ENABLED=1 $(PYTHON) -m uvicorn instagram_app_helper:app --host 127.0.0.1 --port 17374
