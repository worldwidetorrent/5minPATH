#!/usr/bin/env bash
set -euo pipefail

python3 -m rtds.cli.build_snapshots "$@"

