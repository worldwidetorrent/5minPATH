PYTHON ?= python3

.PHONY: test lint collect build-snapshots replay evaluate

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check src tests

collect:
	./scripts/run_collectors.sh

build-snapshots:
	$(PYTHON) -m rtds.cli.build_snapshots

replay:
	$(PYTHON) -m rtds.cli.replay_day

evaluate:
	$(PYTHON) -m rtds.cli.evaluate
