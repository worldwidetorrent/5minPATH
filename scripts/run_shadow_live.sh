#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}src"
python3 -m rtds.cli.run_shadow_live "$@"
