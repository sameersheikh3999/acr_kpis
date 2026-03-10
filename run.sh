#!/usr/bin/env bash
# Run dashboard: API (and built frontend if present) on port 8000.
set -e
cd "$(dirname "$0")"
export PORT=8000
uv run uvicorn api:app --host 0.0.0.0 --port "$PORT"
