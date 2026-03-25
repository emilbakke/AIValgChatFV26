# AI ValgChat FV26

This directory is a public code release of the live `AI ValgChat FV26` application.

The goal is to keep the runtime app as close as possible to the deployed version while leaving out materials that should not be published in a public repository.

## What is included

- `app.py`: main Flask application
- `wsgi.py`: WSGI entrypoint
- `start.sh`: Gunicorn start script
- `requirements.txt`
- `delete_session_by_code.py`
- `templates/index.html`
- `static/styles_v2.css`
- `static/DDC-logo.svg`
- `static/DDC-logo-rosa-2.svg`
- `party_context_map.json`
- `knowledge_ids` (placeholder values only)
- `mp_party_map_2025.json`
- `knowledge/party_facts_canonical_2026.json`
- `knowledge/party_facts_2026/`

## What is intentionally omitted

- scraping scripts
- merge/build/data-preparation scripts
- logs and metrics files
- raw retrieval corpora and source bundles
- principle programmes and other uploaded source files
- internal config/cache directories
- deployment-specific secrets
- participant ethics documents from the live deployment

## Intentional differences from the deployed app

These are the only deliberate differences from the live runtime version:

1. `app_v3.py` is renamed to `app.py`.
2. Real backend URLs are replaced with placeholders.
3. Real collection UUIDs in `knowledge_ids` are replaced with placeholders.
4. Personal contact fields in the frontend are replaced with placeholders.
5. Ethics document links/routes are removed because the documents are not included here.

## Running locally

Provide your own values for:

- `CHAT_API_URL` or `CHAT_API_URLS`
- `WEBUI_API_KEY`
- `PUBLIC_BASE_URL`
- real collection UUIDs in `knowledge_ids`
- optionally `ENABLE_RESEARCH_LOGGING=false` to stop writes to `logs.jsonl`
- optionally `ENABLE_METRICS_LOGGING=false` to stop writes to `metrics.jsonl`
- optionally use runtime flag files:
  - `.disable_research_logging`
  - `.disable_metrics_logging`

Example:

```bash
export CHAT_API_URL="https://your-openwebui.example/api/chat/completions"
export WEBUI_API_KEY="your-api-key"
export PUBLIC_BASE_URL="http://127.0.0.1:8000"
python3 app.py
```

Or:

```bash
bash start.sh
```
