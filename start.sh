#!/usr/bin/env bash
set -euxo pipefail

# Runtime defaults (can be overridden by job-level env vars)
export CHAT_API_URL="${CHAT_API_URL:-https://your-openwebui.example/api/chat/completions}"
export CHAT_API_URLS="${CHAT_API_URLS:-$CHAT_API_URL}"
export MODEL_NAME="${MODEL_NAME:-OpenEuroLLM-Danish-gpu:latest}"
export WEBUI_API_KEY="${WEBUI_API_KEY:-$(tr -d '\r\n' < api_key)}"
export METRICS_LOG_FILE="${METRICS_LOG_FILE:-metrics.jsonl}"
export ENABLE_RESEARCH_LOGGING="${ENABLE_RESEARCH_LOGGING:-true}"
export ENABLE_METRICS_LOGGING="${ENABLE_METRICS_LOGGING:-true}"
export ADMIN_METRICS_TOKEN="${ADMIN_METRICS_TOKEN:-$WEBUI_API_KEY}"
export PUBLIC_BASE_URL="${PUBLIC_BASE_URL:-https://your-public-app.example}"

PORT="${PORT:-8000}"

exec python3 -m gunicorn \
  --worker-class gevent \
  --workers 2 \
  --worker-connections 200 \
  --bind "0.0.0.0:${PORT}" \
  --worker-tmp-dir /tmp \
  --pid /tmp/gunicorn.pid \
  --timeout 600 \
  --capture-output \
  wsgi:app
