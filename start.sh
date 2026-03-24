#!/usr/bin/env bash
set -e

export PYTHONDONTWRITEBYTECODE=${PYTHONDONTWRITEBYTECODE:-1}
export PYTHONUNBUFFERED=${PYTHONUNBUFFERED:-1}

HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8000}

exec python -m uvicorn app.main:app --host "${HOST}" --port "${PORT}"
