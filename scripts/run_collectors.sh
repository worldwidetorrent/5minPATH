#!/usr/bin/env bash
set -euo pipefail

export RTDS_COLLECTION_ENTRYPOINT="scripts/run_collectors.sh"
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}src"
python3 -m rtds.cli.collect "$@"
