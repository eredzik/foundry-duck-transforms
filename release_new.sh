#!/usr/bin/env bash
set -euo pipefail

# Bump patch version in pyproject.toml using Python (avoids sed quoting issues)
python - << 'PY'
from pathlib import Path
import re

path = Path("pyproject.toml")
text = path.read_text()

m = re.search(r'version = "(\d+)\.(\d+)\.(\d+)"', text)
if not m:
    raise SystemExit("Could not find version in pyproject.toml")

major, minor, patch = map(int, m.groups())
new_version = f'{major}.{minor}.{patch + 1}'

new_text = re.sub(
    r'version = "\d+\.\d+\.\d+"',
    f'version = "{new_version}"',
    text,
    count=1,
)
path.write_text(new_text)
print(f"Bumped version to {new_version}")
PY

rm -rf dist/*
uv run python -m build
uv run twine upload dist/*