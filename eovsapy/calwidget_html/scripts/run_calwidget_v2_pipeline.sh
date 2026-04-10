#!/usr/bin/env bash

set -euo pipefail

if [ -f /home/user/.setenv_pyenv38 ]; then
  # shellcheck disable=SC1091
  source /home/user/.setenv_pyenv38
fi

cd "$(dirname "$0")/.."
python scripts/calwidget_v2_server.py
