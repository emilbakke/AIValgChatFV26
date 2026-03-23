"""
Voting Advice Assistant (VAA) – Open WebUI RAG (collections) + SSE streaming + Web search control
----------------------------------------------------------------------------------------------

- Modern UI (template + css)
- Silent RAG injection via Open WebUI collections (files=[{type:"collection", id:"..."}])
- True streaming to browser via Server-Sent Events (SSE) at /api/chat_stream
- Also provides /api/chat (non-streaming JSON) as fallback
- In-memory conversation context per session cookie (follow-up questions work)
- Logs only the latest interaction (one JSON line per request)
- Optional Web search orchestration (works because Open WebUI can web-search via /api/chat/completions)
    - No special /api/tools endpoint required
    - We steer via conditional system messages
"""

import ast
import json
import os
import random
import re
import secrets
import threading
import time
import uuid
from collections import deque
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from PIL import Image, ImageDraw, ImageFont
from flask import Flask, Response, jsonify, make_response, render_template, request, stream_with_context, abort, send_file, has_request_context

# Default backend endpoint (single URL).
DEFAULT_CHAT_API_URL = "https://your-openwebui.example/api/chat/completions"


# Map party aliases to knowledge collection *names*
# These names must match the second element in your knowledge_ids file
PARTY_KB = {
    # Socialdemokratiet
    "socialdemokratiet": "socialdemokratiet",
    "socialdemokratiets": "socialdemokratiet",
    "socialdemokraterne": "socialdemokratiet",
    "socialdemokraternes": "socialdemokratiet",
    "s": "socialdemokratiet",

    # Venstre
    "venstre": "venstre",
    "venstres": "venstre",
    "v": "venstre",

    # SF
    "sf": "sf",
    "sfs": "sf",
    "socialistisk folkeparti": "sf",
    "socialistisk folkepartis": "sf",

    # Radikale
    "radikale": "radikale",
    "radikales": "radikale",
    "radikale venstre": "radikale",
    "radikale venstres": "radikale",
    "b": "radikale",

    # Konservative
    "konservative": "konservative",
    "konservatives": "konservative",
    "det konservative folkeparti": "konservative",
    "det konservative folkepartis": "konservative",
    "c": "konservative",

    # Liberal Alliance
    "liberal alliance": "liberalalliance",
    "liberal alliances": "liberalalliance",
    "la": "liberalalliance",
    "las": "liberalalliance",
    "i": "liberalalliance",

    # Enhedslisten
    "enhedslisten": "enhedslisten",
    "enhedslistens": "enhedslisten",
    "ø": "enhedslisten",

    # Dansk Folkeparti
    "dansk folkeparti": "danskfolkeparti",
    "dansk folkepartis": "danskfolkeparti",
    "df": "danskfolkeparti",
    "dfs": "danskfolkeparti",
    "o": "danskfolkeparti",

    # Danmarksdemokraterne
    "danmarksdemokraterne": "danmarksdemokraterne",
    "danmarksdemokraternes": "danmarksdemokraterne",
    "dd": "danmarksdemokraterne",
    "dds": "danmarksdemokraterne",
    "æ": "danmarksdemokraterne",

    # Moderaterne
    "moderaterne": "moderaterne",
    "moderaternes": "moderaterne",
    "m": "moderaterne",

    # Alternativet
    "alternativet": "alternativet",
    "alternativets": "alternativet",
    "å": "alternativet",

    # Borgernes Parti (if you really have it)
    "borgernes parti": "borgernesparti",
    "borgernes partis": "borgernesparti",
    "borgernesparti": "borgernesparti",
    "borgernespartis": "borgernesparti",
}

# Lightweight preference hints for stage-1 routing on broad vote-advice prompts.
PREFERENCE_HINTS: Dict[str, List[str]] = {
    "lighed": ["enhedslisten", "sf", "socialdemokratiet", "alternativet", "radikale"],
    "fællesskab": ["socialdemokratiet", "sf", "enhedslisten", "radikale"],
    "empati": ["sf", "enhedslisten", "radikale", "alternativet", "socialdemokratiet"],
    "sundhed": ["socialdemokratiet", "sf", "enhedslisten", "venstre", "radikale"],
    "klima": ["alternativet", "sf", "enhedslisten", "radikale", "socialdemokratiet"],
    "velfærd": ["socialdemokratiet", "sf", "enhedslisten", "radikale", "venstre"],
    "lave skatter": ["liberalalliance", "venstre", "konservative"],
    "skattelettelser": ["liberalalliance", "venstre", "konservative"],
    "hårde straffe": ["danskfolkeparti", "danmarksdemokraterne", "konservative", "venstre"],
    "kriminalitet": ["danskfolkeparti", "danmarksdemokraterne", "venstre", "konservative"],
    "flygtninge": ["danskfolkeparti", "danmarksdemokraterne", "venstre", "konservative"],
    "udlændinge": ["danskfolkeparti", "danmarksdemokraterne", "venstre", "konservative"],
}

POLITICS_SCOPE_TERMS: Tuple[str, ...] = (
    "politik",
    "politisk",
    "parti",
    "partier",
    "valg",
    "valget",
    "valgkamp",
    "folketingsvalg",
    "folketing",
    "regering",
    "minister",
    "statsminister",
    "ordfører",
    "ordfoerer",
    "lovforslag",
    "finanslov",
    "skattepolitik",
    "klimapolitik",
    "udlændingepolitik",
    "udlaendingepolitik",
    "velfærd",
    "velfaerd",
    "sundhedspolitik",
    "retspolitik",
    "socialdemokratiet",
    "venstre",
    "radikale",
    "konservative",
    "liberal alliance",
    "enhedslisten",
    "dansk folkeparti",
    "danmarksdemokraterne",
    "moderaterne",
    "alternativet",
    "borgernes parti",
)

OFFTOPIC_STRONG_TERMS: Tuple[str, ...] = (
    "opskrift",
    "opskrifter",
    "madlavning",
    "kage",
    "træning",
    "traening",
    "fitness",
    "programmering",
    "python",
    "javascript",
    "romantik",
    "dating",
    "horoskop",
    "astrologi",
    "medicin",
    "diagnose",
    "behandling",
    "konspirationsteori",
    "flad jord",
)

CONTEXT_DISPLAY_NAME_FALLBACK: Dict[str, str] = {
    "socialdemokratiet": "Socialdemokratiet",
    "venstre": "Venstre",
    "sf": "SF",
    "radikale": "Radikale Venstre",
    "konservative": "Konservative",
    "liberalalliance": "Liberal Alliance",
    "enhedslisten": "Enhedslisten",
    "danskfolkeparti": "Dansk Folkeparti",
    "danmarksdemokraterne": "Danmarksdemokraterne",
    "moderaterne": "Moderaterne",
    "alternativet": "Alternativet",
    "borgernesparti": "Borgernes Parti",
}

FOLLOWUP_TOPIC_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "climate": ("klima", "co2", "grøn", "groen", "omstilling"),
    "immigration": ("indvand", "udlænd", "udlaend", "flygtning", "asyl", "integration"),
    "welfare": ("velfærd", "velfaerd", "velfærd", "sundhed", "psykiatri", "ældre", "aeldre"),
    "tax": ("skat", "skatte", "topskat", "formueskat", "moms", "afgift"),
    "defence": ("forsvar", "sikkerhed", "krig", "nato", "beredskab"),
    "vegan": ("vegan", "veganisme", "dyrevelfærd", "dyrevelfaerd", "dyr"),
}


def load_collection_id_map(path: str = "knowledge_ids") -> Dict[str, str]:
    """
    Parse file with lines like:
      ('uuid', 'socialdemokratiet')
    Returns:
      {"socialdemokratiet": "uuid", ...}
    """
    collection_map: Dict[str, str] = {}
    file_path = Path(path)

    if not file_path.exists():
        return collection_map

    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                uuid, name = ast.literal_eval(s)
                collection_map[str(name)] = str(uuid)
            except Exception:
                continue

    return collection_map


def load_party_context_map(path: str = "party_context_map.json") -> Dict[str, List[str]]:
    """
    JSON format:
    {
      "Socialdemokratiet": ["socialdemokratiet"],
      "Venstre": ["venstre"]
    }
    """
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    out: Dict[str, List[str]] = {}
    for party_name, contexts in raw.items():
        if not isinstance(party_name, str):
            continue
        if isinstance(contexts, str):
            contexts = [contexts]
        if not isinstance(contexts, list):
            continue
        cleaned = [str(c).strip() for c in contexts if str(c).strip()]
        if cleaned:
            out[party_name.strip()] = cleaned
    return out


def load_mp_party_map(path: str = "mp_party_map_2025.json") -> Dict[str, str]:
    """
    Expects structure with top-level "people":
    {
      "people": {
        "Inger Støjberg": {"party_context_key": "danmarksdemokraterne", ...}
      }
    }
    Returns normalized map: {"inger støjberg": "danmarksdemokraterne", ...}
    """
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    people = raw.get("people", {})
    if not isinstance(people, dict):
        return {}

    out: Dict[str, str] = {}
    for person_name, meta in people.items():
        if not isinstance(person_name, str) or not isinstance(meta, dict):
            continue
        party_key = meta.get("party_context_key")
        if isinstance(party_key, str) and party_key.strip():
            out[person_name.strip().lower()] = party_key.strip()
    return out


def load_party_fact_map(root_dir: str = "knowledge/party_facts_2026") -> Dict[str, Dict[str, str]]:
    """
    Loads lightweight fact fields from per-party markdown files.
    Expected path pattern: {root_dir}/{party_context_key}/000_fakta_2026.md
    """
    root = Path(root_dir)
    out: Dict[str, Dict[str, str]] = {}
    if not root.exists() or not root.is_dir():
        return out

    for party_dir in root.iterdir():
        if not party_dir.is_dir():
            continue
        key = party_dir.name.strip()
        md_file = party_dir / "000_fakta_2026.md"
        if not md_file.exists():
            continue
        try:
            text = md_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        facts: Dict[str, str] = {}
        # Keep parser simple and robust to minor formatting variations.
        patterns = {
            "party_name": r"(?mi)^- Parti:\s*(.+?)\s*$",
            "party_letter": r"(?mi)^- Partibogstav:\s*(.+?)\s*$",
            "political_leader": r"(?mi)^- Politisk leder:\s*(.+?)\s*$",
            "chairperson": r"(?mi)^- Partiformand:\s*(.+?)\s*$",
            "founded": r"(?mi)^- Stiftet/grundlagt:\s*(.+?)\s*$",
            "ideology": r"(?mi)^- Politisk ideologi:\s*(.+?)\s*$",
            "source_url": r"(?mi)^Kilde:\s*(https?://\S+)\s*$",
        }
        for field, pat in patterns.items():
            m = re.search(pat, text)
            if m:
                facts[field] = m.group(1).strip()

        # Prefer political leader for user-facing "formand" questions when available.
        if facts.get("political_leader"):
            facts["chairperson"] = facts["political_leader"]

        if facts:
            out[key] = facts
    return out


def load_party_facts_canonical(path: str = "knowledge/party_facts_canonical_2026.json") -> Dict[str, Dict[str, str]]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    parties = raw.get("parties", {})
    if not isinstance(parties, dict):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for key, row in parties.items():
        if not isinstance(key, str) or not isinstance(row, dict):
            continue
        out[key.strip()] = {str(k): str(v) for k, v in row.items() if v is not None}
    return out


def detect_knowledge_bases(text: str, mp_name_party_map: Dict[str, str] | None = None) -> List[str]:
    """
    Detect which KB names should be used, based on party aliases.
    Returns sorted list of KB names like ["socialdemokratiet", "venstre"].
    """
    t = (text or "").lower()
    matched: Set[str] = set()

    # Phrase matches first (avoid single-letter noise)
    for alias, kb in PARTY_KB.items():
        if len(alias) > 2 and alias in t:
            matched.add(kb)

    # Token matches for short aliases:
    # - 2-letter aliases (df, sf, la, dd, ...)
    # - 1-letter aliases only when user wrote explicit uppercase letter (A, B, C, ...)
    tokens = re.findall(r"[a-zæøå]+", t)
    token_set = set(tokens)
    upper_tokens = set(re.findall(r"\b[A-ZÆØÅ]\b", text or ""))

    for alias, kb in PARTY_KB.items():
        if len(alias) == 2 and alias in token_set:
            matched.add(kb)
        elif len(alias) == 1:
            if alias.upper() in upper_tokens:
                matched.add(kb)

    # MP-name matches from curated 2025 mapping (e.g., "Inger Støjberg" -> danmarksdemokraterne)
    if mp_name_party_map:
        for full_name, party_key in mp_name_party_map.items():
            if full_name and full_name in t:
                matched.add(party_key)

    return sorted(matched)


def load_api_key_from_file(path: str = "api_key") -> str:
    """
    Reads an API key from a file that contains the key itself.
    Env var WEBUI_API_KEY takes precedence.
    """
    env_key = os.environ.get("WEBUI_API_KEY", "").strip()
    if env_key:
        return env_key

    p = Path(path)
    if not p.exists():
        return ""

    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _load_preview_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    font_candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for path in font_candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def create_app() -> Flask:
    app = Flask(__name__)
    ethics_dir = (Path(__file__).resolve().parent / "ethics")

    # Core config
    app.config["CHAT_API_URL"] = os.environ.get("CHAT_API_URL", DEFAULT_CHAT_API_URL)
    backend_urls_env = (os.environ.get("CHAT_API_URLS", "") or "").strip()
    if backend_urls_env:
        backend_urls = [u.strip() for u in backend_urls_env.split(",") if u.strip()]
    else:
        backend_urls = [app.config["CHAT_API_URL"]]
    app.config["CHAT_API_URLS"] = backend_urls
    app.config["MODEL_NAME"] = os.environ.get("MODEL_NAME", "OpenEuroLLM-Danish-gpu:latest")
    app.config["PUBLIC_BASE_URL"] = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")

    # API key: env var first, else file "api_key"
    app.config["WEBUI_API_KEY"] = load_api_key_from_file(os.environ.get("WEBUI_API_KEY_FILE", "api_key"))

    # Knowledge ids file
    app.config["KNOWLEDGE_IDS_FILE"] = os.environ.get("KNOWLEDGE_IDS_FILE", "knowledge_ids")
    app.config["PARTY_CONTEXT_MAP_FILE"] = os.environ.get("PARTY_CONTEXT_MAP_FILE", "party_context_map.json")
    app.config["MP_PARTY_MAP_FILE"] = os.environ.get("MP_PARTY_MAP_FILE", "mp_party_map_2025.json")
    app.config["PARTY_FACTS_CANONICAL_FILE"] = os.environ.get(
        "PARTY_FACTS_CANONICAL_FILE",
        "knowledge/party_facts_canonical_2026.json",
    )

    # RAG instruction (system message)
    # Design B: allow own knowledge, but be transparent and avoid direct voting directives.
    app.config["RAG_INSTRUCTION"] = os.environ.get(
        "RAG_INSTRUCTION",
        (
            "Du er en Voting Advice Assistant i forbindelse med det danske folketingsvalg 24/3/2026. "
            "Du må ikke sige til brugeren hvad de skal stemme på. "
            "Du skal i stedet give en matchliste (top 3) baseret på brugerens prioriteringer og svar. "
            "Strukturér dine svar i to dele: (A) Hvad kilderne siger (fra vedlagte partikollektioner), "
            "(B) Fortolkning og baggrund (din generelle viden, tydeligt markeret). "
            "Hvis kilder mangler for et tema, så sig det eksplicit, og hvis du udfylder med baggrundsviden så tag forbehold. "
            "Du må kun foreslå partier, som findes i den tilladte partiliste fra systemet. "
            "Hvis du ikke har tilstrækkelig information til en retvisende top-3, så sig det tydeligt i stedet for at gætte. "
            "Svar altid på dansk."
        ),
    ).strip()
    app.config["CONTEXT_DATE"] = os.environ.get("CONTEXT_DATE", "").strip()
    app.config["CONTEXT_TZ"] = os.environ.get("CONTEXT_TZ", "Europe/Copenhagen").strip() or "Europe/Copenhagen"
    app.config["ELECTION_START_DATE"] = os.environ.get("ELECTION_START_DATE", "2026-02-27")
    app.config["ELECTION_DAY_DATE"] = os.environ.get("ELECTION_DAY_DATE", "2026-03-24")

    # Logging
    app.config["LOG_FILE"] = os.environ.get("LOG_FILE", "logs.jsonl")
    app.config["METRICS_LOG_FILE"] = os.environ.get("METRICS_LOG_FILE", "metrics.jsonl")
    app.config["ADMIN_METRICS_TOKEN"] = (
        os.environ.get("ADMIN_METRICS_TOKEN", "").strip() or app.config["WEBUI_API_KEY"]
    )

    # Conversation memory controls
    app.config["MAX_TURNS"] = int(os.environ.get("MAX_TURNS", "12"))  # user+assistant pairs
    app.config["MAX_CHARS_PER_TURN"] = int(os.environ.get("MAX_CHARS_PER_TURN", "3000"))

    # TLS verify (for https)
    tls_flag = os.environ.get("CHAT_TLS_VERIFY", "true").lower()
    app.config["TLS_VERIFY"] = tls_flag in ("1", "true", "yes")

    # Requests timeout (connect, read)
    app.config["HTTP_TIMEOUT_CONNECT"] = float(os.environ.get("HTTP_TIMEOUT_CONNECT", "10"))
    app.config["HTTP_TIMEOUT_READ"] = float(os.environ.get("HTTP_TIMEOUT_READ", "300"))
    app.config["MAX_INFLIGHT_REQUESTS"] = int(os.environ.get("MAX_INFLIGHT_REQUESTS", "20"))
    app.config["INFLIGHT_QUEUE_WAIT_SECONDS"] = float(os.environ.get("INFLIGHT_QUEUE_WAIT_SECONDS", "120"))
    app.config["SESSION_ACTIVE_TTL_SECONDS"] = int(os.environ.get("SESSION_ACTIVE_TTL_SECONDS", "180"))
    app.config["MAX_TOKENS_FACT"] = int(os.environ.get("MAX_TOKENS_FACT", "120"))
    app.config["MAX_TOKENS_VOTE"] = int(os.environ.get("MAX_TOKENS_VOTE", "220"))
    app.config["MAX_TOKENS_COMPARE"] = int(os.environ.get("MAX_TOKENS_COMPARE", "220"))
    app.config["MAX_TOKENS_DEFAULT"] = int(os.environ.get("MAX_TOKENS_DEFAULT", "180"))
    app.config["ADAPTIVE_SHORT_ANSWERS"] = (
        os.environ.get("ADAPTIVE_SHORT_ANSWERS", "true").strip().lower() in ("1", "true", "yes", "y", "on")
    )
    app.config["BACKEND_FAILOVER_ENABLED"] = (
        os.environ.get("BACKEND_FAILOVER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y", "on")
    )
    app.config["BACKEND_FAILOVER_ON_401"] = (
        os.environ.get("BACKEND_FAILOVER_ON_401", "true").strip().lower() in ("1", "true", "yes", "y", "on")
    )
    app.config["ENABLE_FOLLOWUP_QUESTION"] = (
        os.environ.get("ENABLE_FOLLOWUP_QUESTION", "true").strip().lower() in ("1", "true", "yes", "y", "on")
    )

    # Load collections once at startup
    app.config["COLLECTION_ID_MAP"] = load_collection_id_map(app.config["KNOWLEDGE_IDS_FILE"])
    app.config["PARTY_CONTEXT_MAP"] = load_party_context_map(app.config["PARTY_CONTEXT_MAP_FILE"])
    app.config["MP_NAME_PARTY_MAP"] = load_mp_party_map(app.config["MP_PARTY_MAP_FILE"])
    app.config["PARTY_FACTS_ROOT"] = os.environ.get("PARTY_FACTS_ROOT", "knowledge/party_facts_2026")
    app.config["PARTY_FACT_MAP"] = load_party_fact_map(app.config["PARTY_FACTS_ROOT"])
    app.config["PARTY_FACTS_CANONICAL"] = load_party_facts_canonical(app.config["PARTY_FACTS_CANONICAL_FILE"])
    app.config["FALLBACK_TO_ALL_CONTEXTS"] = os.environ.get("FALLBACK_TO_ALL_CONTEXTS", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    app.config["MAX_COLLECTIONS_PER_REQUEST"] = int(os.environ.get("MAX_COLLECTIONS_PER_REQUEST", "6"))
    app.config["SHORTLIST_SIZE"] = int(os.environ.get("SHORTLIST_SIZE", "4"))
    app.config["BROAD_QUERY_MAX_COLLECTIONS"] = int(os.environ.get("BROAD_QUERY_MAX_COLLECTIONS", "4"))
    app.config["FALLBACK_TO_ALL_CONTEXTS_FOR_BROAD"] = (
        os.environ.get("FALLBACK_TO_ALL_CONTEXTS_FOR_BROAD", "false").strip().lower()
        in ("1", "true", "yes", "y", "on")
    )
    # Keep web-search flow available in code, but disabled by default.
    app.config["ENABLE_WEB_SEARCH"] = os.environ.get("ENABLE_WEB_SEARCH", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )

    # Simple in-memory rate limiting
    rate_limits: Dict[str, float] = {}
    inflight_limit = max(0, int(app.config.get("MAX_INFLIGHT_REQUESTS", 0)))
    inflight_semaphore = threading.BoundedSemaphore(inflight_limit) if inflight_limit > 0 else None
    inflight_count = 0
    inflight_count_lock = threading.Lock()

    # In-memory conversation store: session_id -> list of {"role": "...", "content": "..."}
    conversations: Dict[str, List[Dict[str, str]]] = {}
    # Remember last selected KB contexts per chat session for follow-up questions.
    last_selected_kbs_by_session: Dict[str, List[str]] = {}
    # Track already-used follow-up questions per session to avoid repetition.
    used_followups_by_session: Dict[str, List[str]] = {}
    # Track last follow-up theme per session to avoid same theme twice in a row.
    last_followup_theme_by_session: Dict[str, str] = {}
    # Structured preference profile persisted per session.
    preference_profile_by_session: Dict[str, Dict[str, object]] = {}
    # Track the last pending follow-up question the assistant asked.
    pending_followup_by_session: Dict[str, Dict[str, str]] = {}
    # Track how the most recent short user reply was bound to a pending question.
    last_bound_followup_answer_by_session: Dict[str, Dict[str, str]] = {}
    # Track recently resolved follow-up questions to avoid immediate repetition.
    answered_followups_by_session: Dict[str, List[str]] = {}
    # Session -> participant deletion code (shown to user for later data deletion request).
    session_deletion_codes: Dict[str, str] = {}
    # Guard read/write/replace on logs.jsonl operations.
    log_file_lock = threading.Lock()
    metrics_file_lock = threading.Lock()
    backend_rr_lock = threading.Lock()
    backend_rr_index = 0
    request_metrics: deque[dict] = deque(maxlen=5000)
    request_metrics_lock = threading.Lock()
    request_counters = {
        "request_starts": 0,
        "request_outcomes": 0,
        "prompts_processed": 0,
        "succeeded": 0,
        "failed": 0,
        "busy": 0,
        "backend_errors": 0,
    }
    inflight_peak = 0

    def _new_session_id() -> str:
        # One ID per chat conversation (used in memory + logs)
        return uuid.uuid4().hex

    def _extract_content_text(content_obj) -> str:
        """
        Normalize OpenAI-style content that can be:
        - plain string
        - list of typed blocks [{type: "...", text: "..."}]
        """
        if isinstance(content_obj, str):
            return content_obj
        if isinstance(content_obj, list):
            parts: List[str] = []
            for item in content_obj:
                if isinstance(item, str):
                    parts.append(item)
                    continue
                if isinstance(item, dict):
                    txt = item.get("text")
                    if isinstance(txt, str) and txt:
                        parts.append(txt)
                        continue
                    txt2 = item.get("content")
                    if isinstance(txt2, str) and txt2:
                        parts.append(txt2)
            return "".join(parts)
        return ""

    def _create_deletion_code() -> str:
        return f"DDC-{secrets.token_hex(4).upper()}"

    def _get_or_create_deletion_code(sess_id: str) -> str:
        code = session_deletion_codes.get(sess_id, "")
        if code:
            return code
        code = _create_deletion_code()
        session_deletion_codes[sess_id] = code
        return code

    def _now_log_timestamps() -> tuple[str, str]:
        """
        Return (local_ts, utc_ts) for logs.
        - local_ts uses configured timezone (default Europe/Copenhagen)
        - utc_ts is explicit UTC with Z suffix for portability
        """
        utc_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        tz_name = app.config.get("CONTEXT_TZ", "Europe/Copenhagen")
        try:
            local_ts = datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")
        except Exception:
            local_ts = datetime.now().astimezone().isoformat(timespec="seconds")
        return local_ts, utc_ts

    def _log_interaction(
        sess_id: str,
        prompt: str,
        response_text: str,
        selected_kbs: List[str] | None = None,
        selected_collection_ids: List[str] | None = None,
        interaction_id: str | None = None,
        ttft_ms: int | None = None,
        latency_ms: int | None = None,
        ttft_source: str | None = None,
        backend_url: str | None = None,
    ) -> str:
        iid = interaction_id or uuid.uuid4().hex
        response_links = _extract_urls(response_text)
        local_ts, utc_ts = _now_log_timestamps()
        entry = {
            "timestamp": local_ts,
            "timestamp_utc": utc_ts,
            "interaction_id": iid,
            "session_id": sess_id,
            "deletion_code": session_deletion_codes.get(sess_id, ""),
            "prompt": prompt,
            "response": response_text,
            "selected_kbs": selected_kbs or [],
            "selected_collection_ids": selected_collection_ids or [],
            "news_mode": _is_news_query(prompt),
            "ttft_ms": ttft_ms,
            "latency_ms": latency_ms,
            "ttft_source": ttft_source or "none",
            "response_link_count": len(response_links),
            "response_links": response_links[:10],
            "feedback": "none",
        }
        try:
            with log_file_lock:
                with open(app.config["LOG_FILE"], "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"Warning: could not write log entry: {e}")
        endpoint = request.path if has_request_context() else "unknown"
        _record_request_metric(
            endpoint=endpoint,
            outcome="success",
            session_id=sess_id,
            status_code=200,
            ttft_ms=ttft_ms,
            latency_ms=latency_ms,
            backend_url=backend_url,
            ttft_source=ttft_source,
        )
        return iid

    def _log_session_event(sess_id: str, event_type: str, payload: dict | None = None) -> None:
        local_ts, utc_ts = _now_log_timestamps()
        entry = {
            "timestamp": local_ts,
            "timestamp_utc": utc_ts,
            "session_id": sess_id,
            "deletion_code": session_deletion_codes.get(sess_id, ""),
            "event_type": event_type,
            "event_payload": payload or {},
        }
        try:
            with log_file_lock:
                with open(app.config["LOG_FILE"], "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"Warning: could not write session event: {e}")

    def _append_metrics_log(entry: dict) -> None:
        try:
            with metrics_file_lock:
                with open(app.config["METRICS_LOG_FILE"], "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"Warning: could not write metrics entry: {e}")

    def _record_request_metric(
        endpoint: str,
        outcome: str,
        session_id: str | None = None,
        status_code: int | None = None,
        ttft_ms: int | None = None,
        latency_ms: int | None = None,
        backend_url: str | None = None,
        queue_wait_ms: int | None = None,
        ttft_source: str | None = None,
    ) -> None:
        local_ts, utc_ts = _now_log_timestamps()
        now_epoch = time.time()
        entry = {
            "timestamp": local_ts,
            "timestamp_utc": utc_ts,
            "type": "request_metric",
            "endpoint": endpoint,
            "session_id": session_id or "",
            "outcome": outcome,
            "status_code": status_code,
            "ttft_ms": ttft_ms,
            "latency_ms": latency_ms,
            "backend_url": backend_url or "",
            "queue_wait_ms": queue_wait_ms,
            "ttft_source": ttft_source or "none",
        }
        with request_metrics_lock:
            request_metrics.append(
                {
                    "epoch": now_epoch,
                    "type": "request_metric",
                    "endpoint": endpoint,
                    "outcome": outcome,
                    "status_code": status_code,
                    "ttft_ms": ttft_ms,
                    "latency_ms": latency_ms,
                    "backend_url": backend_url or "",
                }
            )
            request_counters["request_outcomes"] += 1
            if outcome == "success":
                request_counters["succeeded"] += 1
                request_counters["prompts_processed"] += 1
            elif outcome == "busy":
                request_counters["busy"] += 1
                request_counters["failed"] += 1
            else:
                request_counters["failed"] += 1
                if outcome == "backend_error":
                    request_counters["backend_errors"] += 1
        _append_metrics_log(entry)

    def _record_request_start(endpoint: str, session_id: str | None = None) -> None:
        local_ts, utc_ts = _now_log_timestamps()
        now_epoch = time.time()
        entry = {
            "timestamp": local_ts,
            "timestamp_utc": utc_ts,
            "type": "request_start",
            "endpoint": endpoint,
            "session_id": session_id or "",
        }
        with request_metrics_lock:
            request_metrics.append(
                {
                    "epoch": now_epoch,
                    "type": "request_start",
                    "endpoint": endpoint,
                    "session_id": session_id or "",
                }
            )
            request_counters["request_starts"] += 1
        _append_metrics_log(entry)

    def _record_session_state(session_id: str, state: str) -> None:
        local_ts, utc_ts = _now_log_timestamps()
        now_epoch = time.time()
        entry = {
            "timestamp": local_ts,
            "timestamp_utc": utc_ts,
            "type": "session_state",
            "session_id": session_id,
            "state": state,
        }
        with request_metrics_lock:
            request_metrics.append(
                {
                    "epoch": now_epoch,
                    "type": "session_state",
                    "session_id": session_id,
                    "state": state,
                }
            )
        _append_metrics_log(entry)

    def _public_url(path: str = "/") -> str:
        base = (app.config.get("PUBLIC_BASE_URL", "") or "").strip().rstrip("/")
        if base:
            return urljoin(base + "/", path.lstrip("/"))
        if has_request_context():
            root = request.url_root.rstrip("/")
            return urljoin(root + "/", path.lstrip("/"))
        return path

    def _utc_timestamp_to_epoch(value: str) -> float | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None

    def _percentile(values: List[int | float], p: float) -> float | None:
        nums = sorted(float(v) for v in values if isinstance(v, (int, float)))
        if not nums:
            return None
        if len(nums) == 1:
            return nums[0]
        idx = max(0, min(len(nums) - 1, int(round((len(nums) - 1) * p))))
        return nums[idx]

    def _build_admin_metrics_snapshot(window_seconds: int = 300) -> dict:
        now_epoch = time.time()
        with request_metrics_lock:
            current_inflight = inflight_count
            peak_inflight = inflight_peak
        try:
            with metrics_file_lock:
                metric_lines = Path(app.config["METRICS_LOG_FILE"]).read_text(encoding="utf-8").splitlines()
        except Exception:
            metric_lines = []

        all_metric_rows: List[dict] = []
        for line in metric_lines[-20000:]:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                all_metric_rows.append(row)

        recent: List[dict] = []
        session_latest: Dict[str, dict] = {}
        counters = {
            "request_starts": 0,
            "request_outcomes": 0,
            "prompts_processed": 0,
            "succeeded": 0,
            "failed": 0,
            "busy": 0,
            "backend_errors": 0,
        }
        for row in all_metric_rows:
            row_type = row.get("type")
            row_epoch = _utc_timestamp_to_epoch(str(row.get("timestamp_utc", "")))
            if row_type == "request_start":
                counters["request_starts"] += 1
            elif row_type == "request_metric":
                counters["request_outcomes"] += 1
                outcome = row.get("outcome")
                if outcome == "success":
                    counters["succeeded"] += 1
                    counters["prompts_processed"] += 1
                else:
                    counters["failed"] += 1
                    if outcome == "busy":
                        counters["busy"] += 1
                    if outcome == "backend_error":
                        counters["backend_errors"] += 1
            sess_id = str(row.get("session_id", "") or "")
            if sess_id and row_epoch is not None:
                prev = session_latest.get(sess_id)
                if prev is None or row_epoch >= prev.get("_epoch", 0):
                    row_copy = dict(row)
                    row_copy["_epoch"] = row_epoch
                    session_latest[sess_id] = row_copy
            if row_epoch is not None and now_epoch - row_epoch <= window_seconds:
                row_copy = dict(row)
                row_copy["_epoch"] = row_epoch
                recent.append(row_copy)

        ttfts = [row["ttft_ms"] for row in recent if row.get("type") == "request_metric" and isinstance(row.get("ttft_ms"), (int, float))]
        latencies = [row["latency_ms"] for row in recent if row.get("type") == "request_metric" and isinstance(row.get("latency_ms"), (int, float))]
        outcomes = {}
        statuses = {}
        backends = {}
        for row in recent:
            if row.get("type") == "request_metric":
                outcome = row.get("outcome") or "unknown"
                outcomes[outcome] = outcomes.get(outcome, 0) + 1
                code = row.get("status_code")
                if code is not None:
                    statuses[str(code)] = statuses.get(str(code), 0) + 1
                backend_url = row.get("backend_url") or ""
                if backend_url:
                    backends[backend_url] = backends.get(backend_url, 0) + 1

        recent_total = sum(1 for row in recent if row.get("type") == "request_metric")
        recent_failed = sum(
            count for name, count in outcomes.items() if name not in ("success",)
        )
        failure_rate = (recent_failed / recent_total) if recent_total else 0.0
        active_sessions = 0
        completed_sessions_total = 0
        abandoned_sessions = 0
        active_ttl = max(30, int(app.config.get("SESSION_ACTIVE_TTL_SECONDS", 180)))
        for row in session_latest.values():
            state = row.get("state")
            epoch = row.get("_epoch", 0)
            if state in ("completed", "withdrawn", "reset"):
                completed_sessions_total += 1
            elif now_epoch - epoch <= active_ttl:
                active_sessions += 1
            else:
                abandoned_sessions += 1

        return {
            "timestamp": _now_log_timestamps()[0],
            "window_seconds": window_seconds,
            "inflight_current": current_inflight,
            "inflight_peak": peak_inflight,
            "max_inflight_requests": app.config.get("MAX_INFLIGHT_REQUESTS", 0),
            "sessions": {
                "active_current": active_sessions,
                "completed_total": completed_sessions_total,
                "abandoned_current": abandoned_sessions,
                "active_ttl_seconds": active_ttl,
            },
            "totals": counters,
            "recent": {
                "requests": recent_total,
                "failure_rate": failure_rate,
                "outcomes": outcomes,
                "status_codes": statuses,
                "backend_usage": backends,
                "ttft_ms": {
                    "p50": _percentile(ttfts, 0.50),
                    "p95": _percentile(ttfts, 0.95),
                    "max": max(ttfts) if ttfts else None,
                },
                "latency_ms": {
                    "p50": _percentile(latencies, 0.50),
                    "p95": _percentile(latencies, 0.95),
                    "max": max(latencies) if latencies else None,
                },
            },
        }

    def _require_admin_metrics_auth() -> bool:
        expected = (app.config.get("ADMIN_METRICS_TOKEN", "") or "").strip()
        if not expected:
            return False
        provided = (
            request.headers.get("X-Admin-Token", "").strip()
            or request.args.get("token", "").strip()
        )
        return secrets.compare_digest(provided, expected)

    def _update_feedback_in_logs(
        sess_id: str,
        rating: str,
        prompt: str,
        response_text: str,
    ) -> bool:
        """
        Update latest matching interaction line in logs.jsonl with feedback=up/down.
        Matching key: session_id + prompt + response.
        """
        log_path = Path(app.config["LOG_FILE"])
        if not log_path.exists():
            return False

        try:
            with log_file_lock:
                lines = log_path.read_text(encoding="utf-8").splitlines()
        except Exception as e:
            print(f"Warning: could not read log file for feedback update: {e}")
            return False

        rows: List[dict] = []
        for ln in lines:
            try:
                rows.append(json.loads(ln))
            except Exception:
                rows.append({"_raw": ln})

        def _norm(s: str) -> str:
            return re.sub(r"\s+", " ", (s or "").strip())

        norm_prompt = _norm(prompt)
        norm_response = _norm(response_text)

        def _is_updatable(row: dict) -> bool:
            fb = str(row.get("feedback", "none")).lower()
            return fb in ("none", "", "null")

        updated = False

        # Pass 1: strict match (session + prompt + response) for exact target.
        for i in range(len(rows) - 1, -1, -1):
            row = rows[i]
            if not isinstance(row, dict) or "_raw" in row:
                continue
            if row.get("session_id") != sess_id:
                continue
            if _norm(str(row.get("prompt", ""))) != norm_prompt:
                continue
            if norm_response and _norm(str(row.get("response", ""))) != norm_response:
                continue
            if not _is_updatable(row):
                continue
            row["feedback"] = rating
            updated = True
            break

        # Pass 2: fallback match (session + prompt), ignore response mismatch.
        if not updated:
            for i in range(len(rows) - 1, -1, -1):
                row = rows[i]
                if not isinstance(row, dict) or "_raw" in row:
                    continue
                if row.get("session_id") != sess_id:
                    continue
                if _norm(str(row.get("prompt", ""))) != norm_prompt:
                    continue
                if not _is_updatable(row):
                    continue
                row["feedback"] = rating
                updated = True
                break

        # Pass 3: last resort (latest row in session with feedback none).
        if not updated:
            for i in range(len(rows) - 1, -1, -1):
                row = rows[i]
                if not isinstance(row, dict) or "_raw" in row:
                    continue
                if row.get("session_id") != sess_id:
                    continue
                if not _is_updatable(row):
                    continue
                row["feedback"] = rating
                updated = True
                break

        if not updated:
            return False

        try:
            with log_file_lock:
                tmp_path = log_path.with_suffix(log_path.suffix + ".tmp")
                with tmp_path.open("w", encoding="utf-8") as f:
                    for row in rows:
                        if isinstance(row, dict) and "_raw" in row:
                            f.write(str(row["_raw"]) + "\n")
                        else:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
                tmp_path.replace(log_path)
                return True
        except Exception as e:
            print(f"Warning: could not write updated log file: {e}")
            return False

    def _log_feedback_event(
        sess_id: str,
        rating: str,
        interaction_id: str | None = None,
        prompt: str | None = None,
        response_text: str | None = None,
    ) -> None:
        payload = {
            "rating": rating,
            "interaction_id": (interaction_id or "").strip(),
            "prompt": (prompt or "").strip(),
            "response": (response_text or "").strip(),
        }
        _log_session_event(sess_id, "feedback", payload)

    def _delete_session_data(sess_id: str) -> dict:
        """
        Delete all in-memory and on-disk log data tied to a session_id.
        """
        deleted_log_rows = 0
        deletion_code = session_deletion_codes.get(sess_id, "")
        log_path = Path(app.config["LOG_FILE"])
        if log_path.exists():
            try:
                with log_file_lock:
                    lines = log_path.read_text(encoding="utf-8").splitlines()
                    kept: List[str] = []
                    for ln in lines:
                        try:
                            row = json.loads(ln)
                        except Exception:
                            kept.append(ln)
                            continue
                        row_session = row.get("session_id")
                        row_code = row.get("deletion_code") or ""
                        payload_code = ""
                        payload = row.get("event_payload")
                        if isinstance(payload, dict):
                            payload_code = payload.get("deletion_code") or ""
                        if (
                            row_session == sess_id
                            or (deletion_code and (row_code == deletion_code or payload_code == deletion_code))
                        ):
                            deleted_log_rows += 1
                            continue
                        kept.append(json.dumps(row, ensure_ascii=False))
                    tmp_path = log_path.with_suffix(log_path.suffix + ".tmp")
                    tmp_path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
                    tmp_path.replace(log_path)
            except Exception as e:
                print(f"Warning: could not delete session data from log file: {e}")

        # Clear in-memory state
        _clear_session_state(sess_id)
        rate_limits.pop(sess_id, None)
        if deletion_code:
            sessions_with_same_code = [s for s, c in session_deletion_codes.items() if c == deletion_code]
            for sid in sessions_with_same_code:
                _clear_session_state(sid)
                rate_limits.pop(sid, None)
                session_deletion_codes.pop(sid, None)
        else:
            session_deletion_codes.pop(sess_id, None)
        return {"deleted_log_rows": deleted_log_rows}

    def _apply_rate_limit(session_id: str) -> Tuple[bool, str]:
        now = time.time()
        last = rate_limits.get(session_id, 0.0)
        if now - last < 1.0:
            return False, "Too many requests; please wait."
        rate_limits[session_id] = now
        return True, ""

    def _trim_text(s: str, max_chars: int) -> str:
        s = (s or "").strip()
        if len(s) <= max_chars:
            return s
        return s[: max_chars - 20] + "\n…(forkortet)…"

    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip())

    def _clean_followup_question_text(text: str) -> str:
        q = _normalize_text(text)
        q = re.sub(r"^\*+\s*(opfølgende spørgsmål:|follow-up question:)\s*\*+\s*", "", q, flags=re.IGNORECASE)
        q = q.strip("* ").strip()
        if "?" in q:
            q = q.split("?", 1)[0].strip()
        return q

    def _sanitize_followup_question(text: str) -> str:
        raw = _normalize_text(text)
        if not raw:
            return ""
        is_english = "follow-up question:" in raw.lower()
        question = _clean_followup_question_text(raw)
        if not question:
            return ""
        prefix = "**Follow-up question:** **" if is_english else "**Opfølgende spørgsmål:** **"
        return f"{prefix}{question}?**"

    def _ensure_preference_profile(session_id: str) -> Dict[str, object]:
        profile = preference_profile_by_session.get(session_id)
        if isinstance(profile, dict):
            return profile
        profile = {
            "issue_weights": {},
            "issue_notes": {},
            "exclusions": [],
            "recent_updates": [],
            "last_update": "",
        }
        preference_profile_by_session[session_id] = profile
        return profile

    def _clear_session_state(session_id: str) -> None:
        conversations.pop(session_id, None)
        used_followups_by_session.pop(session_id, None)
        last_selected_kbs_by_session.pop(session_id, None)
        last_followup_theme_by_session.pop(session_id, None)
        preference_profile_by_session.pop(session_id, None)
        pending_followup_by_session.pop(session_id, None)
        last_bound_followup_answer_by_session.pop(session_id, None)
        answered_followups_by_session.pop(session_id, None)

    def _store_recent_profile_update(session_id: str, update_text: str) -> None:
        profile = _ensure_preference_profile(session_id)
        recent = profile.setdefault("recent_updates", [])
        if not isinstance(recent, list):
            recent = []
            profile["recent_updates"] = recent
        clean = _normalize_text(update_text)
        if not clean:
            return
        if clean in recent:
            recent.remove(clean)
        recent.append(clean)
        profile["recent_updates"] = recent[-8:]
        profile["last_update"] = clean

    def _set_issue_weight(session_id: str, theme: str, label: str, source_text: str) -> None:
        if theme in ("", "general", "fact", "comparison"):
            return
        profile = _ensure_preference_profile(session_id)
        issue_weights = profile.setdefault("issue_weights", {})
        if not isinstance(issue_weights, dict):
            issue_weights = {}
            profile["issue_weights"] = issue_weights
        issue_weights[theme] = label
        _store_recent_profile_update(session_id, f"{theme}: {label} ({source_text})")

    def _set_issue_note(session_id: str, theme: str, note: str) -> None:
        if theme in ("", "general", "fact", "comparison"):
            return
        profile = _ensure_preference_profile(session_id)
        issue_notes = profile.setdefault("issue_notes", {})
        if not isinstance(issue_notes, dict):
            issue_notes = {}
            profile["issue_notes"] = issue_notes
        issue_notes[theme] = _normalize_text(note)
        _store_recent_profile_update(session_id, f"{theme}: {_normalize_text(note)}")

    def _add_exclusions_from_text(session_id: str, text: str) -> None:
        clean = _normalize_text(text)
        if not clean:
            return
        profile = _ensure_preference_profile(session_id)
        exclusions = profile.setdefault("exclusions", [])
        if not isinstance(exclusions, list):
            exclusions = []
            profile["exclusions"] = exclusions

        lowered = clean.lower()
        exclusion_markers = (
            "vil ikke stemme på",
            "ikke stemme på",
            "ikke have",
            "ikke anbefale",
            "stop med",
            "ikke ",
        )
        if not any(m in lowered for m in exclusion_markers):
            return

        detected = detect_knowledge_bases(clean, app.config.get("MP_NAME_PARTY_MAP", {}) or {})
        for ctx in detected:
            display = CONTEXT_DISPLAY_NAME_FALLBACK.get(ctx, ctx.replace("_", " ").title())
            if display not in exclusions:
                exclusions.append(display)
        if detected:
            profile["exclusions"] = exclusions[-8:]
            _store_recent_profile_update(session_id, f"ekskluderet: {', '.join(profile['exclusions'])}")

    def _infer_importance_label(text: str) -> str:
        q = (text or "").lower()
        if any(m in q for m in ("vigtigst", "allervigtigst", "mest vigtigt")):
            return "vigtigst"
        if any(m in q for m in ("meget vigtigt", "rigtig vigtigt", "meget central")):
            return "meget vigtigt"
        if any(m in q for m in ("vigtigt", "vigtig", "det betyder meget")):
            return "vigtigt"
        if any(m in q for m in ("mindre vigtigt", "lavere prioritet")):
            return "mindre vigtigt"
        if any(m in q for m in ("ikke vigtigt", "ligegyldigt", "uvigtigt")):
            return "ikke vigtigt"
        return ""

    def _looks_like_short_followup_answer(text: str) -> bool:
        q = _normalize_text(text).lower()
        if not q:
            return False
        if len(q.split()) <= 5 and (
            q in {
                "ja",
                "nej",
                "måske",
                "det er vigtigt",
                "meget vigtigt",
                "vigtigt",
                "mindre vigtigt",
                "ikke vigtigt",
                "enig",
                "uenig",
                "begge dele",
            }
            or _infer_importance_label(q)
        ):
            return True
        return False

    def _looks_like_brief_answer(text: str) -> bool:
        q = _normalize_text(text)
        if not q:
            return False
        if len(q.split()) <= 6 and len(q) <= 60:
            return True
        return False

    def _is_repeat_question_complaint(text: str) -> bool:
        q = _normalize_text(text).lower()
        markers = (
            "det har du lige spurgt om",
            "det spurgte du også om",
            "du har allerede spurgt",
            "det har du spurgt om",
            "du spurgte lige om det",
            "you already asked that",
            "you just asked that",
        )
        return any(m in q for m in markers)

    def _recently_answered_followups(session_id: str) -> List[str]:
        recent = answered_followups_by_session.get(session_id, [])
        return recent if isinstance(recent, list) else []

    def _mark_followup_answered(session_id: str, question_text: str) -> None:
        question = _normalize_text(question_text)
        if not question:
            return
        recent = _recently_answered_followups(session_id)
        if question in recent:
            recent.remove(question)
        recent.append(question)
        answered_followups_by_session[session_id] = recent[-12:]

    def _infer_followup_question_type(question_text: str) -> str:
        q = _clean_followup_question_text(question_text).lower()
        if any(m in q for m in ("hvad vægter du højest", "which single policy area", "hvilket område")):
            return "priority_choice"
        if any(m in q for m in ("hvor vigtigt", "how important", "vigtigere", "vægte højere")):
            return "importance_or_tradeoff"
        if any(m in q for m in ("foretrækker du", "do you prefer")):
            return "preference_direction"
        if q.startswith("**opfølgende spørgsmål:** **skal") or q.startswith("**follow-up question:** **should"):
            return "yes_no_tradeoff"
        if any(m in q for m in ("vil du have samme fakta", "same fact for two other parties")):
            return "comparison_offer"
        return "clarification"

    def _infer_followup_expected_answer_mode(question_text: str, question_type: str) -> str:
        q = _clean_followup_question_text(question_text).lower()
        if question_type == "priority_choice":
            return "topic_choice"
        if question_type in ("yes_no_tradeoff", "comparison_offer"):
            return "yes_no"
        if question_type == "importance_or_tradeoff":
            if "hvor vigtigt" in q or "how important" in q:
                return "importance"
            return "yes_no_or_tradeoff"
        if question_type == "preference_direction":
            return "direction"
        if any(m in q for m in ("specifikke", "f.eks.", "for eksempel", "which specific")):
            return "specifics"
        return "open"

    def _interpret_brief_bound_answer(
        theme: str, question_text: str, question_type: str, expected_mode: str, proposition: str, text: str
    ) -> Dict[str, str] | None:
        lowered = _normalize_text(text).lower()
        ql = _clean_followup_question_text(question_text).lower()

        if not _looks_like_brief_answer(text):
            return None

        climate_market_map = {
            "markeds": "markedsdrevne klimaløsninger",
            "marked": "markedsdrevne klimaløsninger",
            "markedsdrevne": "markedsdrevne klimaløsninger",
            "markedsdrevne løsninger": "markedsdrevne klimaløsninger",
            "markedsløsninger": "markedsdrevne klimaløsninger",
            "mest til markedsdrevne": "markedsdrevne klimaløsninger",
            "hælder mest til markedsdrevne": "markedsdrevne klimaløsninger",
            "haelder mest til markedsdrevne": "markedsdrevne klimaløsninger",
            "regler": "hårdere klimaregler",
            "klimaregler": "hårdere klimaregler",
            "hårdere regler": "hårdere klimaregler",
            "strengere regler": "hårdere klimaregler",
            "regulering": "hårdere klimaregler",
        }
        welfare_subareas = {
            "sundhed": "sundhed",
            "uddannelse": "uddannelse",
            "ældre": "ældrepleje",
            "aeldre": "ældrepleje",
            "psykiatri": "psykiatri",
            "børn": "børn og unge",
            "boern": "børn og unge",
            "unge": "børn og unge",
        }
        priority_topics = {
            "skat": "tax",
            "skattelettelser": "tax",
            "velfærd": "welfare",
            "velfaerd": "welfare",
            "udlændingepolitik": "immigration",
            "udlaendingepolitik": "immigration",
            "integration": "immigration",
            "grøn omstilling": "climate",
            "groen omstilling": "climate",
            "klima": "climate",
            "sundhed": "health",
            "forsvar": "defence",
            "frihed": "tax",
        }

        if "hårdere klimaregler" in ql or "markedsdrevne klimaløsninger" in ql:
            for key, value in climate_market_map.items():
                if key in lowered:
                    return {
                        "structured_value": value,
                        "meaning": f"Brugeren foretrækker {value}.",
                        "resolution": "resolved",
                    }

        if "hvilke dele af velfærden" in ql or "velfærdsområder" in ql or expected_mode == "specifics":
            picked = []
            for key, value in welfare_subareas.items():
                if key in lowered and value not in picked:
                    picked.append(value)
            if picked:
                joined = ", ".join(picked)
                return {
                    "structured_value": joined,
                    "meaning": f"Brugeren fremhæver disse velfærdsområder: {joined}.",
                    "resolution": "resolved",
                }

        if question_type == "priority_choice":
            for key, value in priority_topics.items():
                if key in lowered:
                    return {
                        "structured_value": value,
                        "meaning": f"Brugeren valgte {value} som vigtigste næste prioritet.",
                        "resolution": "resolved",
                    }

        if "social retfærdighed" in ql and lowered in {"vigtigt", "meget vigtigt", "ikke vigtigt", "mindre vigtigt"}:
            return {
                "structured_value": lowered,
                "meaning": f"Brugeren angiver, at fokus på social retfærdighed i klima- og velfærdspolitikken er {lowered}.",
                "resolution": "resolved",
            }

        if theme == "tax" and "omfordeling" in ql and lowered in {"vigtigt", "meget vigtigt", "ikke vigtigt", "mindre vigtigt"}:
            return {
                "structured_value": lowered,
                "meaning": f"Brugeren angiver, at omfordeling er {lowered}.",
                "resolution": "resolved",
            }

        return None

    def _summarize_followup_proposition(question_text: str, theme: str, question_type: str) -> str:
        q = _clean_followup_question_text(question_text)
        ql = q.lower()
        if question_type == "yes_no_tradeoff":
            if "økonomisk vækst" in ql and "klima" in ql:
                return "klimahensyn skal vægte højere end økonomisk vækst på kort sigt"
            if "forsvar" in ql and "velfærd" in ql:
                return "mere forsvar er vigtigere end ekstra velfærd"
            return q
        if "omfordeling" in ql:
            return "partiet prioriterer omfordeling af penge i samfundet"
        if "progressivt" in ql or "velhavende bidrager mere" in ql:
            return "velfærdsydelserne finansieres progressivt, så de velhavende bidrager mere"
        if "stærk økonomi" in ql and "finansiere velfærd" in ql:
            return "et parti tager udgangspunkt i en stærk økonomi for at finansiere velfærden"
        if question_type == "comparison_offer":
            return "brugeren bliver spurgt om at sammenligne flere partier direkte"
        if question_type == "preference_direction":
            return f"brugeren skal præcisere retningen på temaet {theme}"
        if question_type == "priority_choice":
            return "brugeren skal vælge hvilket politikområde der betyder mest som næste prioritet"
        if "specifikke" in ql or "f.eks." in ql or "for eksempel" in ql:
            return f"brugeren bliver bedt om at nævne konkrete tiltag eller underemner inden for {theme}"
        return q

    def _infer_question_theme(question_text: str) -> str:
        q = _clean_followup_question_text(question_text).lower()
        if any(m in q for m in ("skat", "skatte", "progressivt", "finansieres", "velhavende bidrager")):
            return "tax"
        if any(m in q for m in ("velfærd", "velfaerd", "sundhed", "ældre", "aeldre", "uddannelse")):
            return "welfare"
        if any(m in q for m in ("indvand", "udlænd", "udlaend", "integration", "flygtning")):
            return "immigration"
        if any(m in q for m in ("forsvar", "sikkerhed", "krig", "nato", "beredskab")):
            return "defence"
        if any(m in q for m in ("klima", "co2", "grøn", "groen", "omstilling")):
            return "climate"
        return _detect_theme_from_text(q)

    def _register_pending_followup(session_id: str, question_text: str, theme: str = "") -> None:
        question = _sanitize_followup_question(question_text)
        if not question or "?" not in question:
            pending_followup_by_session.pop(session_id, None)
            return
        theme_clean = theme or _infer_question_theme(question)
        question_type = _infer_followup_question_type(question)
        pending_followup_by_session[session_id] = {
            "question_text": question,
            "theme": theme_clean,
            "question_type": question_type,
            "expected_answer_mode": _infer_followup_expected_answer_mode(question, question_type),
            "proposition": _summarize_followup_proposition(question, theme_clean, question_type),
        }

    def _extract_pending_followup_from_response(session_id: str, response_text: str) -> None:
        question_line = _extract_last_question_line(response_text)
        if not question_line:
            pending_followup_by_session.pop(session_id, None)
            return
        _register_pending_followup(session_id, question_line)

    def _interpret_followup_answer(session_id: str, user_text: str) -> Dict[str, str] | None:
        pending = pending_followup_by_session.get(session_id)
        if not pending:
            return None
        text = _normalize_text(user_text)
        lowered = text.lower()
        if not text:
            return None

        if _is_repeat_question_complaint(text):
            return {
                "question_text": pending.get("question_text", ""),
                "theme": pending.get("theme", "general"),
                "question_type": pending.get("question_type", "clarification"),
                "expected_answer_mode": pending.get("expected_answer_mode", "open"),
                "proposition": pending.get("proposition", pending.get("question_text", "")),
                "raw_answer": text,
                "structured_value": "",
                "meaning": "Brugeren påpeger, at dette spørgsmål allerede er blevet stillet og ønsker ikke en gentagelse.",
                "resolution": "repeat_complaint",
            }

        is_short = _looks_like_short_followup_answer(text)
        looks_bound = is_short or _looks_like_followup(text)
        if not looks_bound:
            return None

        theme = pending.get("theme", "general")
        question_text = _clean_followup_question_text(str(pending.get("question_text", "") or ""))
        question_type = pending.get("question_type", "clarification")
        expected_mode = pending.get("expected_answer_mode", "open")
        proposition = _normalize_text(str(pending.get("proposition", question_text) or question_text))

        meaning = text
        structured_value = text
        resolution = "resolved"
        brief_interpretation = _interpret_brief_bound_answer(theme, question_text, question_type, expected_mode, proposition, text)
        if brief_interpretation:
            structured_value = brief_interpretation.get("structured_value", text)
            meaning = brief_interpretation.get("meaning", text)
            resolution = brief_interpretation.get("resolution", "resolved")

        if question_type == "yes_no_tradeoff" and lowered not in ("ja", "nej", "måske", "enig", "uenig") and not brief_interpretation:
            structured_value = ""
            meaning = f"Brugeren gav ikke et entydigt ja/nej-svar på spørgsmålet om {proposition}."
            resolution = "ambiguous"

        if lowered in ("ja", "nej", "måske", "enig", "uenig"):
            structured_value = lowered
            if question_type == "yes_no_tradeoff":
                if lowered in ("ja", "enig"):
                    meaning = f"Brugeren bekræfter, at {proposition}."
                elif lowered in ("nej", "uenig"):
                    meaning = f"Brugeren afviser, at {proposition}."
                else:
                    meaning = f"Brugeren er usikker på, om {proposition}."
            elif question_type == "comparison_offer":
                meaning = f"Brugeren svarer {text} på, om samme faktatype skal sammenlignes på tværs af partier."
            elif expected_mode == "specifics":
                if lowered in ("nej", "uenig"):
                    meaning = f"Brugeren har ikke nogen enkelt specifik prioritet inden for {theme}, men ser området mere samlet."
                elif lowered in ("ja", "enig"):
                    meaning = f"Brugeren bekræfter, at der findes specifikke underområder inden for {theme}, som betyder mere, men har ikke navngivet dem endnu."
                    resolution = "partial"
                else:
                    meaning = f"Brugeren er usikker på, om der er én specifik prioritet inden for {theme}."
            else:
                if lowered in ("ja", "enig"):
                    meaning = f"Brugeren bekræfter, at {proposition}."
                elif lowered in ("nej", "uenig"):
                    meaning = f"Brugeren afviser, at {proposition}."
                else:
                    meaning = f"Brugeren er usikker på, om {proposition}."
        importance = _infer_importance_label(text)
        if importance:
            if expected_mode == "specifics":
                structured_value = importance
                meaning = (
                    f"Brugeren svarer ikke med konkrete tiltag, men præciserer i stedet at dette er {importance}: {proposition}."
                )
                resolution = "partial"
            else:
                structured_value = importance
                meaning = f"Brugeren angiver, at dette er {importance}: {proposition}."
        elif question_type == "priority_choice":
            matched_theme = _detect_theme_from_text(text)
            if matched_theme != "general":
                structured_value = matched_theme
                meaning = f"Brugeren valgte {matched_theme} som vigtigste næste prioritet."
            else:
                structured_value = ""
                meaning = (
                    "Brugeren gav ikke et entydigt valg mellem de nævnte prioriteter, men signalerer at flere emner er vigtige."
                )
                resolution = "ambiguous"
        elif question_type == "preference_direction":
            if brief_interpretation:
                meaning = brief_interpretation.get("meaning", text)
            else:
                structured_value = ""
                meaning = f"Brugeren gav ikke et entydigt svar på retningsspørgsmålet om {theme}."
                resolution = "ambiguous"
        elif question_type == "yes_no_tradeoff" and lowered in ("ja", "nej"):
            meaning = f"Brugeren svarer '{text}' på tradeoff-spørgsmålet om {theme}."

        return {
            "question_text": question_text,
            "theme": theme,
            "question_type": question_type,
            "expected_answer_mode": expected_mode,
            "proposition": proposition,
            "raw_answer": text,
            "structured_value": structured_value,
            "meaning": meaning,
            "resolution": resolution,
        }

    def _update_profile_from_followup_answer(session_id: str, interpretation: Dict[str, str]) -> None:
        theme = interpretation.get("theme", "general")
        answer = interpretation.get("raw_answer", "")
        value = interpretation.get("structured_value", "")
        question_type = interpretation.get("question_type", "")
        resolution = interpretation.get("resolution", "resolved")

        if resolution == "repeat_complaint":
            _store_recent_profile_update(session_id, interpretation.get("meaning", answer))
            return
        if resolution == "ambiguous":
            if theme not in ("", "general", "fact", "comparison"):
                _set_issue_note(session_id, theme, interpretation.get("meaning", answer))
            else:
                _store_recent_profile_update(session_id, interpretation.get("meaning", answer))
            return

        if value in ("vigtigst", "meget vigtigt", "vigtigt", "mindre vigtigt", "ikke vigtigt"):
            _set_issue_weight(session_id, theme, value, answer)
        elif question_type == "priority_choice":
            choice_theme = value
            if choice_theme and choice_theme != "general":
                _set_issue_weight(session_id, choice_theme, "vigtigst", answer)
        elif question_type in ("yes_no_tradeoff", "importance_or_tradeoff", "preference_direction"):
            _set_issue_note(session_id, theme, interpretation.get("meaning", answer))
        else:
            _store_recent_profile_update(session_id, interpretation.get("meaning", answer))

    def _update_profile_from_freeform_prompt(session_id: str, user_prompt: str) -> None:
        clean = _normalize_text(user_prompt)
        if not clean:
            return
        _add_exclusions_from_text(session_id, clean)
        importance = _infer_importance_label(clean)
        detected_themes = [
            theme for theme, kws in FOLLOWUP_TOPIC_KEYWORDS.items()
            if theme not in ("general", "fact", "comparison") and any(kw in clean.lower() for kw in kws)
        ]
        for theme in detected_themes[:3]:
            if importance:
                _set_issue_weight(session_id, theme, importance, clean)
            else:
                _set_issue_note(session_id, theme, clean)
        if _is_vote_advice_query(clean) and not detected_themes:
            _store_recent_profile_update(session_id, clean)

    def _register_user_preference_signal(session_id: str, user_prompt: str) -> None:
        interpretation = _interpret_followup_answer(session_id, user_prompt)
        if interpretation:
            last_bound_followup_answer_by_session[session_id] = interpretation
            _update_profile_from_followup_answer(session_id, interpretation)
            _mark_followup_answered(session_id, interpretation.get("question_text", ""))
            pending_followup_by_session.pop(session_id, None)
            return
        last_bound_followup_answer_by_session.pop(session_id, None)
        _update_profile_from_freeform_prompt(session_id, user_prompt)

    def _expand_user_prompt_for_model(session_id: str, user_prompt: str) -> str:
        text = _normalize_text(user_prompt)
        if not text:
            return text
        bound = last_bound_followup_answer_by_session.get(session_id) or {}
        if not isinstance(bound, dict) or not bound:
            return text
        meaning = _normalize_text(str(bound.get("meaning", "") or ""))
        question_text = _clean_followup_question_text(str(bound.get("question_text", "") or ""))
        if not meaning or not question_text:
            return text
        resolution = str(bound.get("resolution", "resolved") or "resolved")
        if resolution == "repeat_complaint":
            return text
        return (
            f"{meaning} Dette er mit svar på spørgsmålet: '{question_text}'. "
            f"Mit korte originale svar var: '{text}'."
        )

    def _local_bound_clarification_response(session_id: str) -> str | None:
        bound = last_bound_followup_answer_by_session.get(session_id) or {}
        if not isinstance(bound, dict) or not bound:
            return None
        resolution = str(bound.get("resolution", ""))
        if resolution not in {"ambiguous", "partial"}:
            return None
        question = _sanitize_followup_question(str(bound.get("question_text", "") or ""))
        if not question:
            return None
        if resolution == "partial":
            return (
                "Dit svar præciserer noget af det, men ikke hele spørgsmålet.\n\n"
                f"{question}"
            )
        return (
            "Dit svar passer ikke helt til det spørgsmål, jeg stillede. Lad os tage det mere præcist.\n\n"
            f"{question}"
        )

    def _render_preference_profile(session_id: str) -> str:
        profile = preference_profile_by_session.get(session_id)
        if not isinstance(profile, dict):
            return ""
        lines: List[str] = []

        issue_weights = profile.get("issue_weights", {})
        if isinstance(issue_weights, dict) and issue_weights:
            pretty = ", ".join(f"{k}: {v}" for k, v in sorted(issue_weights.items()))
            lines.append(f"- Kendte prioriteringer: {pretty}")

        issue_notes = profile.get("issue_notes", {})
        if isinstance(issue_notes, dict) and issue_notes:
            note_lines = [f"{k}: {v}" for k, v in sorted(issue_notes.items()) if v]
            if note_lines:
                lines.append("- Kendte præciseringer: " + " | ".join(note_lines[:4]))

        exclusions = profile.get("exclusions", [])
        if isinstance(exclusions, list) and exclusions:
            lines.append("- Partier brugeren har fravalgt: " + ", ".join(exclusions[:6]))

        recent = profile.get("recent_updates", [])
        if isinstance(recent, list) and recent:
            lines.append("- Seneste opdateringer: " + " | ".join(str(x) for x in recent[-3:]))

        return "\n".join(lines)

    def _has_substantive_preference_profile(session_id: str) -> bool:
        profile = preference_profile_by_session.get(session_id, {})
        if not isinstance(profile, dict):
            return False
        issue_weights = profile.get("issue_weights", {})
        if isinstance(issue_weights, dict) and any(k for k in issue_weights.keys()):
            return True
        issue_notes = profile.get("issue_notes", {})
        if isinstance(issue_notes, dict) and any(k for k in issue_notes.keys()):
            return True
        exclusions = profile.get("exclusions", [])
        if isinstance(exclusions, list) and exclusions:
            return True
        return False

    def _is_greeting_or_readiness(text: str) -> bool:
        q = _normalize_text(text).lower()
        return q in {
            "hej",
            "hejsa",
            "hello",
            "hi",
            "ja",
            "yes",
            "klar",
            "ok",
            "okay",
            "lad os starte",
            "lad os begynde",
        }

    def _local_onboarding_response(session_id: str, user_prompt: str) -> str | None:
        if _has_substantive_preference_profile(session_id):
            return None
        q = _normalize_text(user_prompt).lower()
        if q in {"hej", "hejsa", "hello", "hi"}:
            pending_followup_by_session.pop(session_id, None)
            return (
                "Hej! Jeg er din Voting Advice Assistant til folketingsvalget den 24. marts 2026.\n\n"
                "Jeg kan hjælpe dig med at finde de partier, der bedst matcher dine holdninger. "
                "For at komme i gang skal jeg stille dig et par korte spørgsmål. Er du klar?"
            )
        if q in {"ja", "yes", "klar", "ok", "okay", "lad os starte", "lad os begynde"}:
            first_question = _build_followup_question(session_id, "klima velfærd")
            _register_pending_followup(session_id, first_question, _detect_theme_from_text(first_question))
            return f"Fint. Lad os begynde.\n\n{first_question}"
        return None

    def _covered_followup_themes(session_id: str) -> Set[str]:
        profile = preference_profile_by_session.get(session_id, {})
        covered: Set[str] = set()
        if isinstance(profile, dict):
            issue_weights = profile.get("issue_weights", {})
            if isinstance(issue_weights, dict):
                covered.update(k for k in issue_weights.keys() if k)
            issue_notes = profile.get("issue_notes", {})
            if isinstance(issue_notes, dict):
                covered.update(k for k in issue_notes.keys() if k)
        return {t for t in covered if t not in ("general", "fact", "comparison")}

    def _next_followup_theme_candidates(session_id: str) -> List[str]:
        preferred_order = ["welfare", "tax", "immigration", "health", "defence", "climate"]
        covered = _covered_followup_themes(session_id)
        remaining = [theme for theme in preferred_order if theme not in covered]
        if not remaining:
            return preferred_order
        return remaining

    def _ensure_conversation(session_id: str) -> None:
        if session_id not in conversations:
            conversations[session_id] = []

    def _append_turn(session_id: str, role: str, content: str) -> None:
        _ensure_conversation(session_id)
        conversations[session_id].append(
            {"role": role, "content": _trim_text(content, app.config["MAX_CHARS_PER_TURN"])}
        )
        max_msgs = 2 * app.config["MAX_TURNS"]
        if len(conversations[session_id]) > max_msgs:
            conversations[session_id] = conversations[session_id][-max_msgs:]

    def _infer_kbs_from_history(session_id: str) -> List[str]:
        """
        Infer likely party contexts from recent user turns in the same chat session.
        Useful for follow-up questions like 'Hvornår er partiet stiftet?'.
        """
        _ensure_conversation(session_id)
        recent_user_msgs: List[str] = []
        for turn in reversed(conversations.get(session_id, [])):
            if turn.get("role") == "user":
                recent_user_msgs.append(turn.get("content", ""))
            if len(recent_user_msgs) >= 4:
                break
        if not recent_user_msgs:
            return []
        text = "\n".join(recent_user_msgs)
        return detect_knowledge_bases(text, app.config.get("MP_NAME_PARTY_MAP", {}) or {})

    def _infer_kbs_from_assistant_history(session_id: str) -> List[str]:
        """
        Infer likely contexts from recent assistant turns.
        Helps follow-ups like 'de partier' where party names are only in prior assistant answer.
        """
        _ensure_conversation(session_id)
        recent_assistant_msgs: List[str] = []
        for turn in reversed(conversations.get(session_id, [])):
            if turn.get("role") == "assistant":
                recent_assistant_msgs.append(turn.get("content", ""))
            if len(recent_assistant_msgs) >= 2:
                break
        if not recent_assistant_msgs:
            return []
        text = "\n".join(recent_assistant_msgs)
        return detect_knowledge_bases(text, app.config.get("MP_NAME_PARTY_MAP", {}) or {})

    def _is_in_political_scope(user_prompt: str, session_id: str) -> bool:
        q = (user_prompt or "").strip().lower()
        if not q:
            return False

        # Only block on explicit strong off-topic markers; otherwise allow.
        if any(term in q for term in OFFTOPIC_STRONG_TERMS):
            return False

        return True

    def _out_of_scope_reply() -> str:
        return (
            "Jeg kan kun hjælpe med spørgsmål om dansk politik, partier, kandidater, "
            "valg og politiske emner op til folketingsvalget 24/3/2026.\n\n"
            "Prøv fx: 'Hvilke partier matcher mine holdninger om klima og skat?'"
        )

    def _all_allowed_contexts() -> List[str]:
        party_context_map: Dict[str, List[str]] = app.config.get("PARTY_CONTEXT_MAP", {}) or {}
        allowed_contexts: List[str] = []
        for contexts in party_context_map.values():
            for ctx in contexts:
                if ctx not in allowed_contexts:
                    allowed_contexts.append(ctx)
        return allowed_contexts

    def _display_names_for_allowed_parties() -> List[str]:
        party_context_map: Dict[str, List[str]] = app.config.get("PARTY_CONTEXT_MAP", {}) or {}
        collection_map: Dict[str, str] = app.config.get("COLLECTION_ID_MAP", {}) or {}

        names: List[str] = []
        # Primary source: explicitly configured party display names.
        for party_name in party_context_map.keys():
            pn = str(party_name).strip()
            if pn and pn not in names:
                names.append(pn)

        # Safety net: include contexts found in collection map even if missing in party_context_map.
        for ctx in collection_map.keys():
            c = str(ctx).strip()
            if not c:
                continue
            fallback = CONTEXT_DISPLAY_NAME_FALLBACK.get(c, c.replace("_", " ").title())
            if fallback not in names:
                names.append(fallback)

        return sorted(names)

    def _is_party_list_question(text: str) -> bool:
        q = (text or "").lower()
        markers = (
            "hvilke partier stiller op",
            "hvilke partier er med",
            "hvilke partier findes",
            "hvilke partier har i",
            "hvilke partier kan jeg vælge imellem",
            "hvem stiller op",
            "hvem er med",
        )
        return any(m in q for m in markers)

    def _direct_party_list_answer(user_prompt: str) -> str | None:
        if not _is_party_list_question(user_prompt):
            return None
        party_names = _display_names_for_allowed_parties()
        if not party_names:
            return "Der er ingen partier konfigureret i systemet lige nu."

        lines = ["I dette system er følgende partier tilgængelige:", ""]
        lines.extend([f"- {p}" for p in party_names])
        return "\n".join(lines)

    def _direct_party_presence_answer(user_prompt: str) -> str | None:
        q = (user_prompt or "").lower().strip()
        if not q:
            return None
        # Only trigger on explicit "is party included?" intent.
        presence_patterns = (
            r"^\s*er\b.*\bmed\b",
            r"^\s*findes\b",
            r"\ber der\b.*\bmed\b",
            r"\bmed i (?:systemet|dette system|jeres system)\b",
            r"\bpå (?:listen|partilisten)\b",
        )
        if not any(re.search(p, q) for p in presence_patterns):
            return None
        # Avoid hijacking normal policy/fact/comparison prompts mentioning a party.
        if _is_fact_question(user_prompt) or _is_comparison_query(user_prompt) or _is_vote_advice_query(user_prompt):
            return None

        detected = detect_knowledge_bases(user_prompt, app.config.get("MP_NAME_PARTY_MAP", {}) or {})
        if not detected:
            return None
        names = _display_names_for_allowed_parties()

        # Use first detected context for concise follow-up response.
        ctx = detected[0]
        display = CONTEXT_DISPLAY_NAME_FALLBACK.get(ctx, ctx.replace("_", " ").title())
        if display in names:
            return f"Ja, {display} er med i dette system."
        # If detected but no configured collection/name, be explicit.
        return f"Jeg kan genkende {display}, men partiet er ikke korrekt konfigureret i de aktive kilder endnu."

    def _looks_english(text: str) -> bool:
        q = (text or "").lower()
        english_markers = (
            "please",
            "respond in english",
            "in english",
            "what",
            "which",
            "who",
            "compare",
            "party",
            "parties",
            "thanks",
            "thank you",
        )
        return any(m in q for m in english_markers)

    def _pick_followup_question(
        session_id: str, options: List[str], theme: str = "general", allow_repeat: bool = True
    ) -> str:
        if not options:
            return ""
        used = used_followups_by_session.get(session_id, [])
        answered = set(_recently_answered_followups(session_id))
        remaining = [o for o in options if o not in used and _normalize_text(o) not in answered]
        if not remaining:
            if not allow_repeat:
                return ""
            if used:
                remaining = [o for o in options if o != used[-1] and _normalize_text(o) not in answered]
                if not remaining:
                    remaining = [o for o in options if _normalize_text(o) not in answered]
                    if not remaining:
                        remaining = options[:]
            else:
                remaining = [o for o in options if _normalize_text(o) not in answered]
                if not remaining:
                    remaining = options[:]
            used = []
        choice = random.choice(remaining)
        used.append(choice)
        used_followups_by_session[session_id] = used[-30:]
        last_followup_theme_by_session[session_id] = theme
        return choice

    def _fallback_open_followup(session_id: str, is_english: bool = False) -> str:
        covered = _covered_followup_themes(session_id)
        if is_english:
            if "tax" not in covered:
                return "**Follow-up question:** **Is there anything about taxes or redistribution that matters especially to you?**"
            if "health" not in covered:
                return "**Follow-up question:** **Is there anything specific in healthcare that matters especially to you?**"
            if "immigration" not in covered:
                return "**Follow-up question:** **Is immigration or integration something you want to weigh more heavily in your profile?**"
            if "defence" not in covered:
                return "**Follow-up question:** **Do you want defence or security policy to matter more in your profile?**"
            if "climate" not in covered:
                return "**Follow-up question:** **Is there anything more specific in climate policy that matters especially to you?**"
            return "**Follow-up question:** **Is there another political issue you want to refine or change in your profile?**"

        if "tax" not in covered:
            return "**Opfølgende spørgsmål:** **Er der noget ved skat eller omfordeling, som betyder særligt meget for dig?**"
        if "health" not in covered:
            return "**Opfølgende spørgsmål:** **Er der noget bestemt i sundhedsvæsenet, som betyder særligt meget for dig?**"
        if "immigration" not in covered:
            return "**Opfølgende spørgsmål:** **Er udlændinge- eller integrationspolitik noget, du vil vægte mere i din profil?**"
        if "defence" not in covered:
            return "**Opfølgende spørgsmål:** **Er forsvar eller sikkerhedspolitik noget, du vil lade fylde mere i din profil?**"
        if "climate" not in covered:
            return "**Opfølgende spørgsmål:** **Er der noget mere specifikt i klimapolitikken, som betyder særligt meget for dig?**"
        return "**Opfølgende spørgsmål:** **Er der et andet politisk område, du gerne vil uddybe eller ændre i din profil?**"

    def _build_followup_question(session_id: str, user_prompt: str) -> str:
        q = (user_prompt or "").lower()
        is_english = _looks_english(user_prompt)
        last_theme = last_followup_theme_by_session.get(session_id, "")
        bound = last_bound_followup_answer_by_session.get(session_id, {}) or {}
        resolution = str(bound.get("resolution", ""))
        preferred_themes = _next_followup_theme_candidates(session_id)

        generic_rotation_da = [
            "**Opfølgende spørgsmål:** **Hvad vægter du højest: skattelettelser, stærkere velfærd, strammere udlændingepolitik eller grøn omstilling?**",
            "**Opfølgende spørgsmål:** **Synes du økonomisk vækst er vigtigere end velfærd?**",
            "**Opfølgende spørgsmål:** **Synes du formueskat er en god idé, hvis den bruges til velfærd og sundhed?**",
            "**Opfølgende spørgsmål:** **Skal Danmark bruge flere penge på forsvar, selv hvis det betyder færre midler til velfærd?**",
            "**Opfølgende spørgsmål:** **Er du for mere selvbetaling i sundhedsvæsenet, hvis ventetiderne bliver kortere?**",
        ]
        generic_rotation_en = [
            "**Follow-up question:** **What matters most to you: lower taxes, stronger welfare, stricter immigration policy, or faster green transition?**",
            "**Follow-up question:** **Is economic growth more important to you than welfare expansion?**",
            "**Follow-up question:** **Do you support a wealth tax if the revenue is used for welfare and healthcare?**",
            "**Follow-up question:** **Should Denmark spend more on defence even if it means less spending on welfare?**",
            "**Follow-up question:** **Do you support more out-of-pocket payment in healthcare if waiting times are reduced?**",
        ]

        def _theme_or_rotate(theme: str, options: List[str]) -> str:
            # Avoid same theme twice in a row (except tightly scoped fact/comparison flows).
            if last_theme == theme and theme not in ("fact", "comparison"):
                rotation_pool = generic_rotation_en if is_english else generic_rotation_da
                rotated = _pick_followup_question(session_id, rotation_pool, "general", allow_repeat=False)
                if rotated:
                    return rotated
                return _fallback_open_followup(session_id, is_english)
            picked = _pick_followup_question(session_id, options, theme, allow_repeat=False)
            if picked:
                return picked
            fallback = _pick_followup_question(
                session_id,
                generic_rotation_en if is_english else generic_rotation_da,
                "general",
                allow_repeat=False,
            )
            if fallback:
                return fallback
            return _fallback_open_followup(session_id, is_english)

        if resolution in ("resolved", "ambiguous", "repeat_complaint", "partial"):
            if resolution == "partial" and bound.get("theme") not in ("", "general", "fact", "comparison"):
                theme = str(bound.get("theme"))
                if theme == "climate":
                    return "**Opfølgende spørgsmål:** **Hvilke klimatiltag betyder mest for dig: CO2-afgift, grøn energi eller transport?**"
                if theme == "welfare":
                    return "**Opfølgende spørgsmål:** **Hvilke dele af velfærden betyder mest for dig: sundhed, uddannelse eller ældrepleje?**"
            for theme in preferred_themes:
                if theme == "welfare":
                    return _theme_or_rotate("welfare", [
                        "**Opfølgende spørgsmål:** **Hvilke dele af velfærden betyder mest for dig: sundhed, uddannelse eller ældrepleje?**",
                        "**Opfølgende spørgsmål:** **Vil du acceptere højere skat for bedre sundhed og velfærd?**",
                    ])
                if theme == "tax":
                    return _theme_or_rotate("tax", [
                        "**Opfølgende spørgsmål:** **Synes du skattelettelser er vigtigere end at bruge flere penge på velfærd?**",
                        "**Opfølgende spørgsmål:** **Skal høje indkomster betale mere i skat for at finansiere velfærd?**",
                    ])
                if theme == "immigration":
                    return _theme_or_rotate("immigration", [
                        "**Opfølgende spørgsmål:** **Hvor vigtigt er det for dig, at Danmark har en stram udlændingepolitik?**",
                        "**Opfølgende spørgsmål:** **Er integration vigtigere for dig end at sænke antallet af indvandrere yderligere?**",
                    ])
                if theme == "health":
                    return _theme_or_rotate("health", [
                        "**Opfølgende spørgsmål:** **Synes du der skal være mere selvbetaling i sundhedsvæsenet for at forkorte ventelister?**",
                        "**Opfølgende spørgsmål:** **Skal private aktører spille en større rolle i sundhedsvæsenet, hvis kvaliteten stiger?**",
                    ])
                if theme == "defence":
                    return _theme_or_rotate("defence", [
                        "**Opfølgende spørgsmål:** **Hvor vigtigt er det for dig, at Danmark har en stærk forsvarsindsats?**",
                        "**Opfølgende spørgsmål:** **Skal Danmark bruge flere penge på forsvar, selv hvis det går ud over velfærden?**",
                    ])
                if theme == "climate":
                    return _theme_or_rotate("climate", [
                        "**Opfølgende spørgsmål:** **Foretrækker du hårdere klimaregler eller mere markedsdrevne klimaløsninger?**",
                        "**Opfølgende spørgsmål:** **Skal klimahensyn vægte højere end økonomisk vækst på kort sigt?**",
                    ])

        if is_english:
            if "tax" in q or "skat" in q:
                return _theme_or_rotate("tax", [
                    "**Follow-up question:** **Do you support a wealth tax if the revenue is used for welfare and healthcare?**",
                    "**Follow-up question:** **Do you support tax cuts even if it means less money for welfare?**",
                    "**Follow-up question:** **Should high incomes pay more tax to fund welfare?**",
                ])
            if "defence" in q or "defense" in q or "forsvar" in q or "security" in q:
                return _theme_or_rotate("defence", [
                    "**Follow-up question:** **Should Denmark spend more on defence even if it means less spending on welfare?**",
                    "**Follow-up question:** **Would you accept higher taxes to strengthen defence and preparedness?**",
                ])
            if "health" in q or "sundhed" in q:
                return _theme_or_rotate("health", [
                    "**Follow-up question:** **Do you support more out-of-pocket payment in healthcare if waiting times are reduced?**",
                    "**Follow-up question:** **Should private providers play a larger role in healthcare if quality improves?**",
                ])
            if "climate" in q or "klima" in q:
                return _theme_or_rotate("climate", [
                    "**Follow-up question:** **Should climate action be prioritized even if it slows economic growth in the short term?**",
                    "**Follow-up question:** **Do you prefer stronger regulation on emissions or more market-based climate solutions?**",
                ])
            if "immigration" in q or "udlænd" in q or "udlaend" in q:
                return _theme_or_rotate("immigration", [
                    "**Follow-up question:** **Should Denmark make immigration rules stricter, even if it reduces labor supply?**",
                    "**Follow-up question:** **Is integration policy more important to you than lower immigration numbers?**",
                ])
            if "welfare" in q or "velfærd" in q or "velfaerd" in q:
                return _theme_or_rotate("welfare", [
                    "**Follow-up question:** **Is economic growth more important to you than expanding welfare services?**",
                    "**Follow-up question:** **Should Denmark spend more on welfare even if taxes increase?**",
                ])
            if _is_fact_question(user_prompt) or _is_party_facts_question(user_prompt):
                last_followup_theme_by_session[session_id] = "fact"
                return "**Follow-up question:** **Do you want the same fact for two other parties so you can compare directly?**"
            if _is_comparison_query(user_prompt):
                last_followup_theme_by_session[session_id] = "comparison"
                return "**Follow-up question:** **Which single policy area should I prioritize next in the comparison: tax, climate, immigration, welfare, or health?**"
            return _theme_or_rotate("general", [
                "**Follow-up question:** **What matters most to you: lower taxes, stronger welfare, stricter immigration policy, or faster green transition?**",
                "**Follow-up question:** **Is economic growth more important to you than welfare expansion?**",
            ])

        if "skat" in q:
            return _theme_or_rotate("tax", [
                "**Opfølgende spørgsmål:** **Synes du formueskat er en god idé, hvis pengene går til velfærd?**",
                "**Opfølgende spørgsmål:** **Synes du skattelettelser er vigtigere end at bruge flere penge på velfærd?**",
                "**Opfølgende spørgsmål:** **Skal høje indkomster betale mere i skat for at finansiere velfærd?**",
                "**Opfølgende spørgsmål:** **Skal topskatten sænkes for at øge arbejdsudbuddet, selv hvis staten får færre indtægter?**",
            ])
        if "forsvar" in q or "sikkerhed" in q or "krig" in q or "nato" in q:
            return _theme_or_rotate("defence", [
                "**Opfølgende spørgsmål:** **Skal Danmark bruge flere penge på forsvar, selv hvis det går ud over velfærden?**",
                "**Opfølgende spørgsmål:** **Er du villig til højere skatter for at styrke dansk forsvar og beredskab?**",
                "**Opfølgende spørgsmål:** **Bør forsvarsbudgettet prioriteres højere end nye sociale velfærdsinitiativer?**",
            ])
        if "sundhed" in q or "psykiatri" in q:
            return _theme_or_rotate("health", [
                "**Opfølgende spørgsmål:** **Synes du der skal være mere selvbetaling i sundhedsvæsenet for at forkorte ventelister?**",
                "**Opfølgende spørgsmål:** **Skal private aktører spille en større rolle i sundhedsvæsenet, hvis kvaliteten stiger?**",
                "**Opfølgende spørgsmål:** **Vil du acceptere højere skat for bedre sundhed og velfærd?**",
            ])
        if "klima" in q:
            return _theme_or_rotate("climate", [
                "**Opfølgende spørgsmål:** **Skal klimahensyn vægte højere end økonomisk vækst på kort sigt?**",
                "**Opfølgende spørgsmål:** **Foretrækker du hårdere klimaregler eller mere markedsdrevne klimaløsninger?**",
            ])
        if "udlænd" in q or "udlaend" in q or "indvand" in q:
            return _theme_or_rotate("immigration", [
                "**Opfølgende spørgsmål:** **Bør Danmark føre en strammere udlændingepolitik, selv hvis det kan koste arbejdskraft?**",
                "**Opfølgende spørgsmål:** **Er integration vigtigere for dig end at sænke antallet af indvandrere yderligere?**",
                "**Opfølgende spørgsmål:** **Skal Danmark modtage flere flygtninge, hvis kommunerne samtidig får flere ressourcer?**",
            ])
        if "velfærd" in q or "velfaerd" in q:
            return _theme_or_rotate("welfare", [
                "**Opfølgende spørgsmål:** **Er økonomisk vækst vigtigere for dig end at udvide velfærden?**",
                "**Opfølgende spørgsmål:** **Vil du acceptere højere skat for bedre sundhed og velfærd?**",
            ])
        if _is_fact_question(user_prompt) or _is_party_facts_question(user_prompt):
            last_followup_theme_by_session[session_id] = "fact"
            return "**Opfølgende spørgsmål:** **Vil du have samme fakta for to andre partier, så du kan sammenligne direkte?**"
        if _is_comparison_query(user_prompt):
            last_followup_theme_by_session[session_id] = "comparison"
            return (
                "**Opfølgende spørgsmål:** **Hvilket område skal jeg sammenligne næste gang: skat, klima, "
                "udlændinge, velfærd eller sundhed?**"
            )
        return _theme_or_rotate("general", generic_rotation_da)

    def _detect_theme_from_text(text: str) -> str:
        q = (text or "").lower()
        for theme, kws in FOLLOWUP_TOPIC_KEYWORDS.items():
            if any(kw in q for kw in kws):
                return theme
        return "general"

    def _declared_themes_in_history(session_id: str) -> Set[str]:
        _ensure_conversation(session_id)
        declared: Set[str] = set()
        for turn in conversations.get(session_id, []):
            if turn.get("role") != "user":
                continue
            t = (turn.get("content") or "").lower()
            for theme, kws in FOLLOWUP_TOPIC_KEYWORDS.items():
                if any(kw in t for kw in kws):
                    declared.add(theme)
        return declared

    def _extract_last_question_line(text: str) -> str:
        lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
        for ln in reversed(lines):
            if "?" in ln:
                return ln
        return ""

    def _looks_corrupted_followup_question(text: str) -> bool:
        q = _normalize_text(text)
        if not q:
            return False
        if q.count("?") > 1:
            return True
        if q.endswith("**") or q.endswith("*"):
            return True
        if re.search(r"\?[A-Za-zÆØÅæøå]{2,}\?", q):
            return True
        if "opfølgende spørgsmål:" in q.lower() and "**" in q and not q.strip().startswith("**Opfølgende spørgsmål:**"):
            return True
        return False

    def _strip_followup_answer_options(text: str) -> str:
        src = (text or "").rstrip()
        if not src:
            return src

        lines = src.splitlines()
        q_idx = -1
        for i in range(len(lines) - 1, -1, -1):
            if "?" in lines[i]:
                q_idx = i
                break
        if q_idx == -1 or q_idx == len(lines) - 1:
            return src

        head = lines[: q_idx + 1]
        tail = lines[q_idx + 1 :]
        cleaned_tail: List[str] = []
        skipping_options = True

        for raw in tail:
            line = raw.rstrip()
            stripped = line.strip()
            if not stripped:
                if not skipping_options:
                    cleaned_tail.append(line)
                continue

            option_like = (
                len(stripped) <= 60
                and (
                    raw.startswith("    ")
                    or raw.startswith("\t")
                    or stripped.startswith("- ")
                    or stripped.startswith("* ")
                    or re.match(r"^\d+[.)]\s+", stripped)
                    or stripped.lower() in {
                        "meget vigtigt",
                        "vigtigt",
                        "hverken eller",
                        "uvigtigt",
                        "slet ikke vigtigt",
                        "jeg ved ikke",
                        "ja",
                        "nej",
                    }
                )
            )
            if skipping_options and option_like:
                continue
            skipping_options = False
            cleaned_tail.append(line)

        rebuilt = "\n".join(head + cleaned_tail).strip()
        return rebuilt

    def _strip_vote_advice_closure(text: str) -> str:
        src = (text or "").strip()
        if not src:
            return src

        banned_markers = (
            "profil nu fuldt defineret",
            "profil er nu fuldt defineret",
            "jeg har nu stillet alle relevante spørgsmål",
            "jeg har stillet alle relevante spørgsmål",
            "der er ikke flere spørgsmål",
            "der er intet mere jeg kan bidrage med",
            "tak for din deltagelse",
            "i denne omgang",
            "jeg er tydeligvis i en loop",
            "samtalen er afsluttet",
            "profilen er færdig",
        )

        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", src) if p.strip()]
        kept: List[str] = []
        for p in paragraphs:
            low = p.lower()
            if any(marker in low for marker in banned_markers):
                continue
            kept.append(p)

        cleaned = "\n\n".join(kept).strip()
        if not cleaned:
            return src
        return cleaned

    def _render_bound_followup_acknowledgment(session_id: str) -> str:
        return ""

    def _bound_followup_scope_instruction(session_id: str) -> str:
        bound = last_bound_followup_answer_by_session.get(session_id) or {}
        if not isinstance(bound, dict) or not bound:
            return ""
        theme = str(bound.get("theme", "general") or "general")
        if theme in {"", "general", "fact", "comparison"}:
            return ""
        proposition = _normalize_text(str(bound.get("proposition", "") or ""))
        question_text = _clean_followup_question_text(str(bound.get("question_text", "") or ""))
        other_themes = [t for t in ("climate", "welfare", "tax", "immigration", "health", "defence") if t != theme]
        return (
            "Denne tur er tematisk låst til brugerens seneste opfølgende svar.\n"
            f"Det konkrete spørgsmål var: {question_text}\n"
            f"Det konkrete tema er: {theme}\n"
            f"Den konkrete proposition er: {proposition}\n"
            "Du må i denne tur kun fortolke brugerens svar i relation til dette konkrete spørgsmål, tema og proposition. "
            "Du må ikke omskrive svaret til et andet politisk tema, og du må ikke udlede nye præferencer om andre politikområder. "
            f"Du må især ikke skifte over til disse andre temaer: {', '.join(other_themes)}. "
            "Hvis du er i tvivl, så hold fortolkningen snæver og bogstavelig."
        )

    def _prepend_bound_followup_acknowledgment(response_text: str, session_id: str, user_prompt: str) -> str:
        if not (_is_vote_advice_query(user_prompt) or _is_vote_advice_followup(session_id, user_prompt)):
            return response_text
        body = (response_text or "").strip()
        body = re.sub(r"^(?:\s*Noteret:.*(?:\n|$))+", "", body, flags=re.IGNORECASE).strip()
        return body

    def _replace_followup_block(text: str, replacement: str) -> str:
        src = (text or "").rstrip()
        markers = ("**opfølgende spørgsmål:**", "**follow-up question:**", "opfølgende spørgsmål:", "follow-up question:")
        src_l = src.lower()
        last_idx = -1
        for marker in markers:
            idx = src_l.rfind(marker)
            if idx > last_idx:
                last_idx = idx
        if last_idx != -1:
            head = src[:last_idx].rstrip()
            head_lines = head.splitlines()
            while head_lines:
                trailing = head_lines[-1].strip()
                if trailing in {"*", "**", "***", "_", "__", "___"}:
                    head_lines.pop()
                    continue
                break
            head = "\n".join(head_lines).rstrip()
            sep = "\n\n" if head else ""
            return f"{head}{sep}{replacement}".strip()
        return ""

    def _replace_last_question_line(text: str, replacement: str) -> str:
        block_replaced = _replace_followup_block(text, replacement)
        if block_replaced:
            return block_replaced
        src = (text or "").rstrip()
        lines = src.splitlines()
        for i in range(len(lines) - 1, -1, -1):
            if "?" in lines[i]:
                lines[i] = replacement
                return "\n".join(lines).strip()
        sep = "\n\n" if src else ""
        return f"{src}{sep}{replacement}".strip()

    def _strip_trailing_question_region(text: str) -> str:
        src = (text or "").rstrip()
        if not src:
            return src

        block_replaced = _replace_followup_block(src, "")
        if block_replaced != "":
            return block_replaced.strip()

        lines = src.splitlines()
        cut_idx = None
        for i in range(len(lines) - 1, -1, -1):
            stripped = lines[i].strip().lower()
            if not stripped:
                continue
            if "?" in lines[i]:
                cut_idx = i
                break
            if stripped in {"*", "**", "***", "_", "__", "___"}:
                cut_idx = i
                continue
            break

        if cut_idx is None:
            return src.strip()

        kept = lines[:cut_idx]
        while kept and not kept[-1].strip():
            kept.pop()
        while kept and kept[-1].strip() in {"*", "**", "***", "_", "__", "___"}:
            kept.pop()
        return "\n".join(kept).strip()

    def _strip_vote_followup_tail(text: str) -> str:
        src = (text or "").strip()
        if not src:
            return src

        src_l = src.lower()
        markers = ("opfølgende spørgsmål:", "follow-up question:")
        cut_positions = [src_l.find(marker) for marker in markers if src_l.find(marker) != -1]
        if cut_positions:
            base = src[: min(cut_positions)].rstrip()
            base = _strip_trailing_question_region(base)
            return base.strip()

        paragraphs = [p for p in re.split(r"\n\s*\n", src) if p.strip()]
        if paragraphs:
            last = paragraphs[-1].strip()
            if "?" in last:
                paragraphs = paragraphs[:-1]
        cleaned = "\n\n".join(p.strip() for p in paragraphs if p.strip()).strip()
        if cleaned:
            return cleaned
        return _strip_trailing_question_region(src)

    def _ensure_followup_question(response_text: str, user_prompt: str, session_id: str) -> str:
        if not app.config.get("ENABLE_FOLLOWUP_QUESTION", True):
            _extract_pending_followup_from_response(session_id, response_text or "")
            return response_text or ""
        text = _strip_vote_advice_closure(_strip_followup_answer_options((response_text or "").strip()))
        if not text:
            pending_followup_by_session.pop(session_id, None)
            return text
        tail = text[-240:].lower()
        generic_markers = (
            "finjusterer top-3",
            "én ekstra prioritet",
            "flere oplysninger om dine prioriteringer",
            "mere præcis matchliste",
        )
        bound_followup = last_bound_followup_answer_by_session.get(session_id) or {}
        should_force_next_followup = True
        is_vote_flow = _is_vote_advice_query(user_prompt) or _is_vote_advice_followup(session_id, user_prompt)
        if is_vote_flow:
            replacement = _sanitize_followup_question(_build_followup_question(session_id, user_prompt).strip())
            base = _strip_vote_followup_tail(text)
            sep = "\n\n" if base else ""
            updated = f"{base}{sep}{replacement}".strip()
            _register_pending_followup(session_id, replacement, _detect_theme_from_text(replacement))
            return updated

        if "?" in tail:
            last_q = _extract_last_question_line(text)
            last_q_l = last_q.lower()
            last_theme = _detect_theme_from_text(last_q_l)
            declared_themes = _declared_themes_in_history(session_id)
            asks_importance_again = (
                "hvor vigtigt" in last_q_l
                or "hvor meget betyder" in last_q_l
                or "how important" in last_q_l
            )
            # If model asks a generic/redundant end-question, replace it.
            repeated_answered = _normalize_text(last_q) in set(_recently_answered_followups(session_id))
            corrupted_question = _looks_corrupted_followup_question(last_q)
            if should_force_next_followup or any(m in tail for m in generic_markers) or (asks_importance_again and last_theme in declared_themes) or repeated_answered or corrupted_question:
                replacement = _sanitize_followup_question(_build_followup_question(session_id, user_prompt).strip())
                updated = _replace_last_question_line(text, replacement)
                _register_pending_followup(session_id, replacement, _detect_theme_from_text(replacement))
                return updated
            # Otherwise keep model's own question.
            _extract_pending_followup_from_response(session_id, text)
            return text
        # Avoid adding follow-up to hard refusal/safety-only messaging.
        if "jeg kan kun hjælpe med spørgsmål om dansk politik" in tail:
            pending_followup_by_session.pop(session_id, None)
            return text
        followup = _sanitize_followup_question(_build_followup_question(session_id, user_prompt).strip())
        sep = "\n\n" if not text.endswith("\n") else "\n"
        updated = f"{text}{sep}{followup}"
        _register_pending_followup(session_id, followup, _detect_theme_from_text(followup))
        return updated

    def _is_broad_query(text: str) -> bool:
        q = (text or "").lower()
        # If a specific party or MP is mentioned, treat as focused.
        if detect_knowledge_bases(text, app.config.get("MP_NAME_PARTY_MAP", {}) or {}):
            return False

        broad_markers = (
            "hvem skal jeg stemme på",
            "hvilket parti",
            "hvilke partier",
            "hvem passer jeg til",
            "valgtest",
            "valgkompas",
            "sammenlign partier",
            "sammenlign de danske partier",
            "hvad mener partierne",
            "hvilket parti passer",
            "hvilket parti matcher",
        )
        return any(m in q for m in broad_markers)

    def _is_vote_advice_query(text: str) -> bool:
        q = (text or "").lower()
        intent_markers = (
            "hvem skal jeg stemme på",
            "hvilket parti",
            "hvilke partier",
            "hvem passer jeg til",
            "valgkompas",
            "valgtest",
            "stemme på",
        )
        preference_markers = (
            "jeg går op i",
            "mine værdier",
            "vigtige værdier",
            "for mig er",
            "jeg prioriterer",
            "det vigtigste for mig",
            "jeg synes",
            "jeg mener",
            "jeg foretrækker",
            "for mig er det vigtigst",
            "primært",
        )
        return any(m in q for m in intent_markers) or any(m in q for m in preference_markers)

    def _is_vote_advice_followup(session_id: str, text: str) -> bool:
        q = (text or "").lower()
        if session_id in pending_followup_by_session and (
            _looks_like_short_followup_answer(text) or _looks_like_brief_answer(text) or _looks_like_followup(text)
        ):
            return True
        if session_id in last_bound_followup_answer_by_session and (
            _looks_like_short_followup_answer(text) or _looks_like_brief_answer(text)
        ):
            return True
        if not _looks_like_followup(text):
            return False
        if not any(
            m in q
            for m in (
                "jeg synes",
                "jeg mener",
                "jeg foretrækker",
                "for mig",
                "vigtig",
                "vigtigst",
                "primært",
                "mest",
                "hellere",
            )
        ):
            return False
        _ensure_conversation(session_id)
        prev_user_msgs = [m.get("content", "") for m in conversations.get(session_id, []) if m.get("role") == "user"]
        if len(prev_user_msgs) < 2:
            return False
        return _is_vote_advice_query(prev_user_msgs[-2])

    def _is_comparison_query(text: str) -> bool:
        q = (text or "").lower()
        markers = (
            "sammenlign",
            "hvad er forskellen",
            "hvad er forskel",
            "forskel på",
            "vs",
            "kontra",
            "op mod",
            "sammenhold",
        )
        return any(m in q for m in markers)

    def _looks_like_followup(text: str) -> bool:
        t = (text or "").lower()
        followup_markers = (
            "partiet",
            "partier",
            "det parti",
            "de partier",
            "de nævnte",
            "det",
            "den",
            "og hvornår",
            "og hvad",
            "hvornår er partiet",
            "hvad med",
            "ok",
            "tak",
            "jo det gør",
            "prøv igen",
            "kan du tjekke igen",
            "er du sikker",
            "jeg går op i",
            "vigtige værdier",
            "mine værdier",
            "for mig er",
            "jeg prioriterer",
        )
        return any(m in t for m in followup_markers)

    def _shortlist_contexts(user_prompt: str, session_id: str, allowed_contexts: List[str], limit: int) -> List[str]:
        """
        Stage-1 routing:
        Cheap heuristic shortlist before collection retrieval.
        """
        if not allowed_contexts or limit <= 0:
            return []

        scores: Dict[str, int] = {ctx: 0 for ctx in allowed_contexts}
        prompt = (user_prompt or "")
        prompt_l = prompt.lower()
        mp_map = app.config.get("MP_NAME_PARTY_MAP", {}) or {}

        # Strong signal: direct alias/party/MP detection in current user prompt.
        direct = detect_knowledge_bases(prompt, mp_map)
        for kb in direct:
            if kb in scores:
                scores[kb] += 10

        # Secondary signal: alias token presence in prompt.
        for alias, kb in PARTY_KB.items():
            if kb not in scores:
                continue
            alias_l = alias.lower()
            if len(alias_l) > 2 and alias_l in prompt_l:
                scores[kb] += 3
            elif len(alias_l) == 2 and alias_l in set(re.findall(r"[a-zæøå]+", prompt_l)):
                scores[kb] += 2

        # Follow-up signal from recent history.
        hist = _infer_kbs_from_history(session_id)
        for kb in hist:
            if kb in scores:
                scores[kb] += 4

        # Session continuity signal.
        for kb in last_selected_kbs_by_session.get(session_id, []):
            if kb in scores:
                scores[kb] += 2

        # Preference-signal from broad voter preference text.
        for phrase, hinted_contexts in PREFERENCE_HINTS.items():
            if phrase in prompt_l:
                for kb in hinted_contexts:
                    if kb in scores:
                        scores[kb] += 3

        ranked = sorted(allowed_contexts, key=lambda c: (-scores.get(c, 0), allowed_contexts.index(c)))
        positive = [ctx for ctx in ranked if scores.get(ctx, 0) > 0]
        if positive:
            return positive[:limit]
        return ranked[:limit]

    def _selected_collections(user_prompt: str, session_id: str) -> Tuple[List[Dict[str, str]], List[str]]:
        """
        Returns:
          - files payload for Open WebUI RAG collections
          - list of selected KB names (for debugging)
        """
        id_map: Dict[str, str] = app.config.get("COLLECTION_ID_MAP", {}) or {}
        allowed_contexts = _all_allowed_contexts()
        is_broad = _is_broad_query(user_prompt)

        max_default = max(1, app.config.get("MAX_COLLECTIONS_PER_REQUEST", 6))
        max_broad = max(1, app.config.get("BROAD_QUERY_MAX_COLLECTIONS", 4))
        shortlist_size = max(1, app.config.get("SHORTLIST_SIZE", 4))
        max_collections = min(max_default, max_broad) if is_broad else max_default
        if _is_vote_advice_query(user_prompt):
            max_collections = min(max_collections, 3)
        if _is_comparison_query(user_prompt):
            max_collections = max(max_collections, 4)

        # Stage-1: quick shortlist/routing (no heavy retrieval yet).
        kb_names = detect_knowledge_bases(user_prompt, app.config.get("MP_NAME_PARTY_MAP", {}) or {})
        if not kb_names:
            kb_names = _infer_kbs_from_history(session_id)
        if not kb_names and _looks_like_followup(user_prompt):
            kb_names = _infer_kbs_from_assistant_history(session_id)
        if not kb_names and _looks_like_followup(user_prompt):
            kb_names = last_selected_kbs_by_session.get(session_id, [])[:]

        if kb_names:
            shortlisted = [kb for kb in kb_names if kb in allowed_contexts]
        else:
            shortlist_limit = min(shortlist_size, max_collections)
            shortlisted = _shortlist_contexts(user_prompt, session_id, allowed_contexts, shortlist_limit)

        if not shortlisted and app.config.get("FALLBACK_TO_ALL_CONTEXTS", True):
            if is_broad and not app.config.get("FALLBACK_TO_ALL_CONTEXTS_FOR_BROAD", False):
                shortlisted = allowed_contexts[: min(shortlist_size, max_collections)]
            else:
                shortlisted = allowed_contexts[:]

        # Stage-2: deep retrieval only on selected shortlist.
        selected_contexts: List[str] = []
        for kb in shortlisted:
            if kb in allowed_contexts and kb not in selected_contexts:
                selected_contexts.append(kb)
            if len(selected_contexts) >= max_collections:
                break

        files: List[Dict[str, str]] = []
        for kb in selected_contexts:
            col_id = id_map.get(kb)
            if col_id:
                files.append({"type": "collection", "id": col_id})

        if selected_contexts:
            last_selected_kbs_by_session[session_id] = selected_contexts[:]

        return files, selected_contexts

    def _is_party_facts_question(text: str) -> bool:
        q = (text or "").lower()
        return any(
            k in q
            for k in (
                "formand",
                "formænd",
                "partiformand",
                "partiformænd",
                "leder",
                "ledere",
                "stiftet",
                "grundlagt",
                "historie",
                "ideologi",
            )
        )

    def _is_election_date_question(text: str) -> bool:
        q = (text or "").lower()
        markers = (
            "hvornår er der valg",
            "hvornår er valget",
            "hvornår er folketingsvalg",
            "hvornår skal vi stemme",
            "hvornår skal danskerne til valg",
            "hvad er valgdatoen",
            "valgdagen",
        )
        return any(m in q for m in markers)

    def _direct_election_answer(user_prompt: str) -> str | None:
        if not _is_election_date_question(user_prompt):
            return None
        election_day_iso = str(app.config.get("ELECTION_DAY_DATE", "2026-03-24"))
        election_start_iso = str(app.config.get("ELECTION_START_DATE", "2026-02-27"))

        def _fmt_dk(iso_date: str) -> str:
            try:
                d = datetime.strptime(iso_date, "%Y-%m-%d")
                return f"{d.day}/{d.month}/{d.year}"
            except Exception:
                return iso_date

        return (
            f"Der er folketingsvalg den {_fmt_dk(election_day_iso)}.\n"
            f"I denne app er valgkampen sat fra {_fmt_dk(election_start_iso)} til {_fmt_dk(election_day_iso)}."
        )

    def _direct_party_facts_answer(user_prompt: str, selected_contexts: List[str], session_id: str) -> str | None:
        """
        Deterministic answer path for short party-fact questions.
        Avoids LLM hallucinations for formand/founded/ideology follow-ups.
        """
        q = (user_prompt or "").lower()
        _ensure_conversation(session_id)
        prev_user_msgs = [m.get("content", "") for m in conversations.get(session_id, []) if m.get("role") == "user"]
        prev_user_prompt = prev_user_msgs[-2] if len(prev_user_msgs) >= 2 else ""

        # Allow follow-up prompts to inherit fact field from previous user question.
        is_fact_turn = _is_party_facts_question(user_prompt)
        is_fact_followup = _looks_like_followup(user_prompt) and _is_fact_question(prev_user_prompt)
        if not is_fact_turn and not is_fact_followup:
            return None

        canonical_map: Dict[str, Dict[str, str]] = app.config.get("PARTY_FACTS_CANONICAL", {}) or {}
        fact_map: Dict[str, Dict[str, str]] = app.config.get("PARTY_FACT_MAP", {}) or {}
        if not canonical_map and not fact_map:
            return None

        def _field_and_label(prompt_q: str) -> Tuple[str, str]:
            if any(k in prompt_q for k in ("formand", "formænd", "partiformand", "partiformænd", "leder", "ledere")):
                return "chairperson", "Partiformand"
            if any(k in prompt_q for k in ("stiftet", "grundlagt", "hvornår")):
                return "founded", "Stiftet/grundlagt"
            if "ideologi" in prompt_q:
                return "ideology", "Politisk ideologi"
            return "", ""

        field, label = _field_and_label(q)
        if not field and is_fact_followup:
            field, label = _field_and_label((prev_user_prompt or "").lower())
        if not field:
            return None

        selected = selected_contexts[:]
        if not selected:
            selected = _all_allowed_contexts()

        # If user asks for "the other parties", answer for all minus previously selected contexts.
        asks_other_parties = any(
            m in q
            for m in (
                "de andre",
                "andre partier",
                "øvrige partier",
                "resten af partierne",
                "adnre",  # common typo observed in logs
            )
        )
        if asks_other_parties:
            previous_selected = last_selected_kbs_by_session.get(session_id, [])[:]
            all_ctx = _all_allowed_contexts()
            remaining = [ctx for ctx in all_ctx if ctx not in previous_selected]
            if remaining:
                selected = remaining

        def _clean_fact_value(v: str) -> str:
            vv = (v or "").strip()
            if not vv:
                return ""
            bad_tokens = ("ikke fundet", "not found", "ukendt")
            low = vv.lower()
            if any(t in low for t in bad_tokens):
                return ""
            return vv

        lines: List[str] = []
        missing = 0
        for ctx in selected:
            c = canonical_map.get(ctx, {})
            f = fact_map.get(ctx, {})
            party_name = (c.get("party_name_da") or f.get("party_name") or ctx).strip()

            value = ""
            if field == "chairperson":
                value = (
                    _clean_fact_value(c.get("leader_for_qa", ""))
                    or _clean_fact_value(c.get("politisk_leder", ""))
                    or _clean_fact_value(c.get("formand", ""))
                    or _clean_fact_value(f.get("chairperson", ""))
                )
            elif field == "founded":
                value = _clean_fact_value(c.get("stiftet", "")) or _clean_fact_value(f.get("founded", ""))
            elif field == "ideology":
                value = _clean_fact_value(c.get("ideologi", "")) or _clean_fact_value(f.get("ideology", ""))
            else:
                value = _clean_fact_value(f.get(field, ""))

            if not value:
                missing += 1
                continue
            lines.append(f"- {party_name}: {value}")

        if not lines:
            return "Det fremgår ikke af de vedlagte kilder."

        # Keep it compact and source-grounded.
        intro = f"Det fremgår af de vedlagte kilder ({label}):"
        out = [intro, "", *lines]
        if missing:
            out.append("")
            out.append("Bemærk: For nogle valgte partier fremgik feltet ikke eksplicit af de lokale faktakilder.")
        return "\n".join(out)

    def _build_messages(session_id: str, user_prompt: str, files: List[Dict[str, str]], selected_contexts: List[str]) -> List[Dict[str, str]]:
        """
        OpenAI-style messages list.
        - Always inject base RAG instruction as system message.
        """
        _ensure_conversation(session_id)
        msgs: List[Dict[str, str]] = []

        instruction = (app.config["RAG_INSTRUCTION"] or "").strip()
        if instruction:
            msgs.append({"role": "system", "content": instruction})

        def _current_context_date() -> str:
            # Optional manual override for testing/reproducibility.
            forced = (app.config.get("CONTEXT_DATE") or "").strip()
            if forced:
                return forced
            tz_name = app.config.get("CONTEXT_TZ", "Europe/Copenhagen")
            try:
                return datetime.now(ZoneInfo(tz_name)).date().isoformat()
            except Exception:
                return datetime.now().date().isoformat()

        current_date = _current_context_date()

        msgs.append(
            {
                "role": "system",
                "content": (
                    f"Dagens dato i denne app er {current_date}. "
                    f"Dansk valgkamp er i gang fra {app.config['ELECTION_START_DATE']} til {app.config['ELECTION_DAY_DATE']} (valgdagen). "
                    "Ved relative tidsord som 'i går' og 'seneste uge' skal du regne ud fra denne dato. "
                    "Du må ikke antage at vi er i 2024, medmindre en specifik kilde eksplicit angiver 2024."
                ),
            }
        )

        profile_text = _render_preference_profile(session_id)
        if profile_text:
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Aktuel struktureret brugerprofil fra tidligere i samtalen:\n"
                        f"{profile_text}\n"
                        "Bevar denne profil som udgangspunkt. Nye svar skal normalt tolkes som præciseringer eller opdateringer, "
                        "ikke som at hele brugerens profil starter forfra."
                    ),
                }
            )

        bound_followup = last_bound_followup_answer_by_session.get(session_id)
        if isinstance(bound_followup, dict) and bound_followup:
            resolution = bound_followup.get("resolution", "resolved")
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Brugerens seneste besked er et direkte svar på dit forrige opfølgende spørgsmål.\n"
                        f"Spørgsmål: {bound_followup.get('question_text', '')}\n"
                        f"Tema: {bound_followup.get('theme', 'general')}\n"
                        f"Status: {resolution}\n"
                        f"Fortolkning: {bound_followup.get('meaning', bound_followup.get('raw_answer', ''))}\n"
                        "Du skal først reagere på dette svar og opdatere vurderingen ud fra det, før du evt. stiller et nyt opfølgende spørgsmål. "
                        "Du må ikke omskrive dette til et andet politisk tema eller udlede nye præferencer, som ikke følger direkte af spørgsmålet. "
                        "Den seneste brugerbesked er allerede bundet til det konkrete spørgsmål internt i appen. "
                        "Du må derfor ikke bruge din første sætning på mekanisk at gentage den samme fortolkning igen."
                    ),
                }
            )
            scope_lock = _bound_followup_scope_instruction(session_id)
            if scope_lock:
                msgs.append({"role": "system", "content": scope_lock})
            msgs.append(
                {
                    "role": "user",
                    "content": (
                        "Fortolkning af mit seneste korte svar: "
                        f"{bound_followup.get('meaning', bound_followup.get('raw_answer', ''))}"
                    ),
                }
            )
            msgs.append(
                {
                    "role": "user",
                    "content": (
                        "Jeg svarer på dette konkrete spørgsmål: "
                        f"'{_clean_followup_question_text(bound_followup.get('question_text', ''))}'. "
                        f"Mit korte svar er: '{bound_followup.get('raw_answer', '')}'. "
                        "Du må ikke omskrive spørgsmålet til et andet politisk spørgsmål."
                    ),
                }
            )
        msgs.append(
            {
                "role": "system",
                "content": (
                    "Du har ikke adgang til internettet i denne app. "
                    "Du må ikke påstå at du har søgt på nettet, browsed websites eller verificeret live-kilder. "
                    "Du må ikke inkludere hyperlinks eller URL'er i svaret."
                ),
            }
        )
        msgs.append(
            {
                "role": "system",
                "content": (
                    "Svar kortfattet og direkte. Undgå lange forklaringer. "
                    "Brug korte afsnit/punktform og fokusér kun på det nødvendige."
                ),
            }
        )

        party_context_map: Dict[str, List[str]] = app.config.get("PARTY_CONTEXT_MAP", {}) or {}
        context_to_party_names: Dict[str, List[str]] = {}
        for party_name, ctxs in party_context_map.items():
            for ctx in ctxs:
                context_to_party_names.setdefault(ctx, []).append(party_name)

        if party_context_map:
            party_names = sorted([p for p in party_context_map.keys() if p.strip()])
            allowed_text = ", ".join(party_names)
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Tilladte partier i dette system er kun: "
                        f"{allowed_text}. "
                        "Foreslå kun partier fra denne liste."
                    ),
                }
            )

        # Clarify pronoun follow-ups: "de partier" should refer to currently selected contexts, not all parties.
        q_lower = (user_prompt or "").lower()
        if selected_contexts and any(m in q_lower for m in ("de partier", "de nævnte partier", "de samme partier", "dem")):
            selected_party_names: List[str] = []
            for ctx in selected_contexts:
                for pn in context_to_party_names.get(ctx, []):
                    if pn not in selected_party_names:
                        selected_party_names.append(pn)
            if selected_party_names:
                msgs.append(
                    {
                        "role": "system",
                        "content": (
                            "I denne opfølgning henviser 'de partier' til følgende partier: "
                            + ", ".join(selected_party_names)
                            + "."
                        ),
                    }
                )

        def _is_fact_followup_prompt() -> bool:
            if not _looks_like_followup(user_prompt):
                return False
            _ensure_conversation(session_id)
            # Previous user prompt (excluding current one, which is already appended before _build_messages).
            prev_user_msgs = [m.get("content", "") for m in conversations.get(session_id, []) if m.get("role") == "user"]
            if len(prev_user_msgs) < 2:
                return False
            previous_user_prompt = prev_user_msgs[-2]
            return _is_fact_question(previous_user_prompt)

        if _is_vote_advice_query(user_prompt) or _is_vote_advice_followup(session_id, user_prompt):
            has_substantive_preferences = _has_substantive_preference_profile(session_id)
            if not has_substantive_preferences:
                msgs.append(
                    {
                        "role": "system",
                        "content": (
                            "Brugeren har endnu ikke givet tilstrækkelige politiske præferencer til en matchliste. "
                            "Du må derfor IKKE foreslå partier, top-3, matchlister eller partinavne endnu. "
                            "Svar i stedet kort og naturligt, anerkend brugerens seneste svar, og stil præcis ét enkelt opfølgende politisk spørgsmål. "
                            "Spørgsmålet skal være almindeligt formuleret uden svarmuligheder eller survey-lister."
                        ),
                    }
                )
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Brugeren ønsker valgrådgivning. Når brugeren giver værdier/prioriteter, skal du behandle det "
                        "som en implicit anmodning om anbefaling og give en top-3 blandt de tilladte partier. "
                        "Når samtalen allerede indeholder kendte præferencer, skal du opdatere fra den eksisterende profil i stedet for at starte forfra. "
                        "Hvis brugeren svarer kort på et opfølgende spørgsmål (fx 'ja', 'nej', 'det er vigtigt'), skal du tydeligt reagere på det svar, "
                        "forklare hvad det ændrer, og kun derefter evt. stille næste spørgsmål. "
                        "Din første reaktion skal holde sig til præcis det emne og den proposition, som det forrige opfølgende spørgsmål handlede om. "
                        "Du må ikke omskrive et kort svar til en anden politisk præference eller et andet tema. "
                        "Hvis det forrige spørgsmål handlede om velfærd, må du ikke omtale svaret som et spørgsmål om skat, forsvar, indvandring eller andre emner. "
                        "Hvis det forrige spørgsmål handlede om klima, må du ikke omtale svaret som et spørgsmål om skat, forsvar, indvandring eller andre emner. "
                        "Hvis brugerens korte svar ikke faktisk besvarer hele spørgsmålet, skal du sige det eksplicit og omformulere spørgsmålet mere præcist i stedet for bare at gå videre. "
                        "Hvis brugeren siger at du allerede har stillet spørgsmålet, skal du anerkende gentagelsen og skifte til et andet relevant spørgsmål i stedet for at tolke det som en ny politisk præference. "
                        "Du må aldrig skrive eller antyde, at brugerens profil er færdig, fuldt defineret eller afsluttet. "
                        "Du må aldrig skrive, at der ikke er flere spørgsmål, at samtalen er slut, eller at brugeren er færdig. "
                        "Brugeren kan altid fortsætte, præcisere eller ændre sin profil. "
                        "Store ændringer i top-3 kræver ny information, som faktisk retfærdiggør ændringen. "
                        "Stil opfølgende spørgsmål som almindelige spørgsmål. "
                        "Du må ikke vise svarmuligheder, bullets, multiple-choice-lister eller survey-lignende svarmenuer under spørgsmålene. "
                        "Svar i kort format: "
                        "1) Kort reaktion på brugerens seneste svar (1 linje), "
                        "2) Kort opsummering af brugerens prioriteter (1 linje), "
                        "3) Top 3 partier med 1-2 linjer begrundelse hver, "
                        "4) Kort usikkerhed/notat om manglende kilder. "
                        "Undgå lange generelle overblik over mange partier. "
                        "Hvis brugerens input er for uklart, stil højst 2 konkrete opklarende spørgsmål."
                    ),
                }
            )

        if _is_comparison_query(user_prompt):
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Brugeren ønsker en sammenligning. "
                        "Svar i en markdown-tabel med kolonner: "
                        "Parti | Skat | Klima | Udlændinge | Sundhed | Velfærd | EU | Retspolitik | Landdistrikter | Kildegrundlag. "
                        "Hold hver celle kort (maks 1 linje) og marker tydeligt hvis data mangler."
                    ),
                }
            )

        if app.config.get("ENABLE_WEB_SEARCH", False) and _is_news_turn(session_id, user_prompt):
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Dette er en nyhedsforespørgsel. "
                        "Brug kun verificerbare, aktuelle kilder. "
                        "Du må aldrig simulere søgning, opfinde artikler, eller skrive at søgning er simuleret. "
                        "Svarformat SKAL indeholde en sektion 'Kilder' med direkte klikbare URL'er (fuld https://...). "
                        "Angiv dato ved hver kilde (YYYY-MM-DD hvis muligt). "
                        "Hvis du ikke kan give verificerbare links, skriv eksplicit at links ikke kunne verificeres."
                    ),
                }
            )

        # For concrete party facts (e.g. chairperson/history/date questions), force strict grounding.
        is_fact_question = _is_fact_question(user_prompt) or _is_fact_followup_prompt()
        if _is_party_facts_question(user_prompt) and selected_contexts:
            fact_map: Dict[str, Dict[str, str]] = app.config.get("PARTY_FACT_MAP", {}) or {}
            lines: List[str] = []
            for ctx in selected_contexts:
                facts = fact_map.get(ctx, {})
                if not facts:
                    continue
                party_name = facts.get("party_name") or ctx
                chair = facts.get("chairperson")
                founded = facts.get("founded")
                source_url = facts.get("source_url")
                chunks: List[str] = [f"- {party_name} ({ctx})"]
                if chair:
                    chunks.append(f"partiformand: {chair}")
                if founded:
                    chunks.append(f"stiftet: {founded}")
                if source_url:
                    chunks.append(f"kilde: {source_url}")
                lines.append("; ".join(chunks))
            if lines:
                msgs.append(
                    {
                        "role": "system",
                        "content": (
                            "Supplerende faktakontekst (kuraterede lokale partifakta):\n"
                            + "\n".join(lines)
                            + "\nBrug disse fakta kun hvis de matcher spørgsmålet; ellers brug vedlagte kilder."
                        ),
                    }
                )

        if is_fact_question:
            msgs.append(
                {
                    "role": "system",
                    "content": (
                        "Dette er et faktaspørgsmål om et parti. "
                        "Svar KUN ud fra de vedlagte kilder/kollektioner. "
                        "Hvis svaret ikke fremgår eksplicit af kilderne, skal du skrive: "
                        "'Det fremgår ikke af de vedlagte kilder.' "
                        "Dette gælder også spørgsmål om formænd/partiformænd/ledere (flertal eller ental). "
                        "Du må ikke bruge generel baggrundsviden, ikke gætte, og ikke opfinde navne eller årstal."
                    ),
                }
            )

        history = list(conversations[session_id])

        # Retrieval query expansion for factual party questions.
        # This improves recall in collection search without changing the visible UI prompt.
        if history and selected_contexts:
            if is_fact_question:
                terms: List[str] = []
                for ctx in selected_contexts:
                    terms.append(ctx)
                    for pn in context_to_party_names.get(ctx, []):
                        terms.append(pn)
                    # Include known aliases from PARTY_KB for this context key
                    for alias, kb in PARTY_KB.items():
                        if kb == ctx and len(alias) >= 2:
                            terms.append(alias)

                # de-duplicate while preserving order
                seen_terms = set()
                dedup_terms: List[str] = []
                for t in terms:
                    tt = str(t).strip()
                    if not tt:
                        continue
                    key = tt.lower()
                    if key in seen_terms:
                        continue
                    seen_terms.add(key)
                    dedup_terms.append(tt)

                if dedup_terms and isinstance(history[-1], dict) and history[-1].get("role") == "user":
                    retrieval_query = (
                        f"query: {user_prompt}. "
                        f"parti: {', '.join(dedup_terms[:8])}. "
                        "fokus: formand formænd partiformand partiformænd leder ledere historie stiftet grundlagt ideologi."
                    )
                    expanded = (
                        f"{history[-1].get('content', '')}\n\n"
                        "[Retrieval query expansion]\n"
                        "Fokus: faktasvar fra vedlagte kilder.\n"
                        f"Søgetermer: {', '.join(dedup_terms[:20])}, formand, formænd, partiformand, partiformænd, leder, ledere.\n"
                        f"{retrieval_query}"
                    )
                    history[-1] = {"role": "user", "content": expanded}

        msgs.extend(history)
        return msgs

    def _is_fact_question(text: str) -> bool:
        q = (text or "").lower()
        return any(
            k in q
            for k in (
                "hvem er",
                "formand",
                "formænd",
                "partiformand",
                "partiformænd",
                "leder",
                "ledere",
                "historie",
                "hvornår",
                "stiftet",
                "grundlagt",
                "ideologi",
                "fakta",
            )
        )

    def _generation_options(user_prompt: str = "") -> dict:
        def _max_tokens() -> int:
            if _is_fact_question(user_prompt):
                return app.config.get("MAX_TOKENS_FACT", 120)
            if _is_comparison_query(user_prompt):
                return app.config.get("MAX_TOKENS_COMPARE", 260)
            if _is_vote_advice_query(user_prompt):
                return app.config.get("MAX_TOKENS_VOTE", 220)
            return app.config.get("MAX_TOKENS_DEFAULT", 220)

        max_tokens = max(64, int(_max_tokens()))
        if app.config.get("ADAPTIVE_SHORT_ANSWERS", True) and inflight_limit > 0:
            with inflight_count_lock:
                pressure = float(inflight_count) / float(inflight_limit)
            # Mild adaptation only, to avoid noticeable quality drop.
            if pressure >= 0.85:
                max_tokens = max(80, int(max_tokens * 0.75))
            elif pressure >= 0.65:
                max_tokens = max(96, int(max_tokens * 0.88))
        # For factual questions, prefer deterministic behavior.
        if _is_fact_question(user_prompt):
            return {"temperature": 0.0, "top_p": 1.0, "max_tokens": max_tokens}
        # For fresh/news-style questions, keep responses grounded.
        if _is_news_query(user_prompt):
            return {"temperature": 0.1, "top_p": 0.9, "max_tokens": max_tokens}
        # For vote-advice responses, allow a bit of flexibility to avoid bland echoing.
        if _is_vote_advice_query(user_prompt):
            return {"temperature": 0.2, "top_p": 0.9, "max_tokens": max_tokens}
        # Default settings (can be overridden by env vars).
        temperature = float(os.environ.get("TEMPERATURE", "0.0"))
        top_p = float(os.environ.get("TOP_P", "0.9"))
        return {"temperature": temperature, "top_p": top_p, "max_tokens": max_tokens}

    def _headers() -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if app.config["WEBUI_API_KEY"]:
            h["Authorization"] = f"Bearer {app.config['WEBUI_API_KEY']}"
        return h

    def _is_news_query(text: str) -> bool:
        q = (text or "").lower()
        markers = (
            "nyheder",
            "seneste",
            "nyt om",
            "i går",
            "idag",
            "i dag",
            "for nylig",
            "opdatering",
            "hvad skete",
            "hvad er sket",
        )
        return any(m in q for m in markers)

    def _is_news_turn(session_id: str, text: str) -> bool:
        if _is_news_query(text):
            return True
        if not _looks_like_followup(text):
            return False
        _ensure_conversation(session_id)
        prev_user_msgs = [m.get("content", "") for m in conversations.get(session_id, []) if m.get("role") == "user"]
        if len(prev_user_msgs) < 2:
            return False
        previous_user_prompt = prev_user_msgs[-2]
        return _is_news_query(previous_user_prompt)

    def _extract_urls(text: str) -> List[str]:
        if not text:
            return []
        return re.findall(r"https?://[^\s)\]]+", text)

    def _remove_urls_from_text(text: str) -> str:
        if not text:
            return ""
        cleaned = str(text)
        # Markdown links: [text](https://...) -> text
        cleaned = re.sub(r"\[([^\]]+)\]\((https?://[^)\s]+)\)", r"\1", cleaned)
        # Angle-bracket URLs
        cleaned = re.sub(r"<https?://[^>\s]+>", "[link removed]", cleaned)
        # Plain URLs
        cleaned = re.sub(r"https?://[^\s)\]]+", "[link removed]", cleaned)
        return cleaned

    def _has_disallowed_news_content(text: str) -> bool:
        t = (text or "").lower()
        blocked_phrases = (
            "simuleret",
            "simulerede",
            "sandsynligvis ville",
            "jeg har ikke adgang til en live nyhedsstrøm",
            "baseret på en simuleret søgning",
        )
        if any(p in t for p in blocked_phrases):
            return True
        if re.search(r"\b2024\b", t):
            return True
        return False

    def _is_url_reachable(url: str) -> bool:
        """
        Best-effort URL reachability check.
        Some sites block HEAD; fallback to lightweight GET.
        """
        try:
            r = requests.head(
                url,
                allow_redirects=True,
                timeout=(4, 8),
                verify=app.config["TLS_VERIFY"],
                headers={"User-Agent": "DDC-VAA-LinkCheck/1.0"},
            )
            if r.status_code < 400:
                return True
            if r.status_code not in (403, 405):
                return False
        except Exception:
            pass
        try:
            r = requests.get(
                url,
                allow_redirects=True,
                timeout=(4, 10),
                verify=app.config["TLS_VERIFY"],
                headers={"User-Agent": "DDC-VAA-LinkCheck/1.0"},
                stream=True,
            )
            return r.status_code < 400
        except Exception:
            return False

    def _reachable_urls(urls: List[str], limit: int = 6) -> List[str]:
        out: List[str] = []
        seen = set()
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            if _is_url_reachable(u):
                out.append(u)
            if len(out) >= limit:
                break
        return out

    def _timeout_tuple() -> Tuple[float, float]:
        return (app.config["HTTP_TIMEOUT_CONNECT"], app.config["HTTP_TIMEOUT_READ"])

    def _ordered_backend_urls() -> List[str]:
        nonlocal backend_rr_index
        urls = app.config.get("CHAT_API_URLS", []) or [app.config["CHAT_API_URL"]]
        if len(urls) <= 1:
            return urls
        with backend_rr_lock:
            start = backend_rr_index % len(urls)
            backend_rr_index = (backend_rr_index + 1) % len(urls)
        return urls[start:] + urls[:start]

    def _is_retryable_backend_status(status_code: int) -> bool:
        if status_code == 401:
            return bool(app.config.get("BACKEND_FAILOVER_ON_401", True))
        return status_code in (429, 500, 502, 503, 504)

    def _post_chat_with_failover(payload: dict, stream: bool = False) -> requests.Response:
        urls = _ordered_backend_urls()
        failover_enabled = bool(app.config.get("BACKEND_FAILOVER_ENABLED", True))
        last_exc: Exception | None = None
        for i, url in enumerate(urls):
            try:
                r = requests.post(
                    url,
                    json=payload,
                    headers=_headers(),
                    stream=stream,
                    timeout=_timeout_tuple(),
                    verify=app.config["TLS_VERIFY"],
                )
                # Retry another backend on transient HTTP statuses.
                if (
                    failover_enabled
                    and r.status_code >= 400
                    and _is_retryable_backend_status(r.status_code)
                    and i < len(urls) - 1
                ):
                    body = (r.text[:240] if r.text else "")
                    print(f"Backend transient HTTP {r.status_code} on {url}; trying next backend. body={body}")
                    continue
                r.raise_for_status()
                setattr(r, "ddc_backend_url", url)
                print(f"Backend selected: {url} stream={stream}")
                return r
            except requests.HTTPError as exc:
                resp = getattr(exc, "response", None)
                code = resp.status_code if resp is not None else 0
                if failover_enabled and _is_retryable_backend_status(code) and i < len(urls) - 1:
                    body = (resp.text[:240] if resp is not None and resp.text else "")
                    print(f"Backend retryable HTTPError {code} on {url}; trying next backend. body={body}")
                    last_exc = exc
                    continue
                raise
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ReadTimeout,
            ) as exc:
                if failover_enabled and i < len(urls) - 1:
                    print(f"Backend connection/timeout error on {url}; trying next backend. err={repr(exc)}")
                    last_exc = exc
                    continue
                raise
        if last_exc:
            raise last_exc
        raise requests.exceptions.RequestException("No backend URL available")

    def _try_acquire_inflight_slot() -> bool:
        nonlocal inflight_count, inflight_peak
        if inflight_semaphore is None:
            return True
        max_wait = max(0.0, float(app.config.get("INFLIGHT_QUEUE_WAIT_SECONDS", 0.0)))
        if max_wait <= 0:
            acquired = inflight_semaphore.acquire(blocking=False)
            if acquired:
                with inflight_count_lock:
                    inflight_count += 1
                    inflight_peak = max(inflight_peak, inflight_count)
            return acquired

        deadline = time.time() + max_wait
        while time.time() <= deadline:
            if inflight_semaphore.acquire(blocking=False):
                with inflight_count_lock:
                    inflight_count += 1
                    inflight_peak = max(inflight_peak, inflight_count)
                return True
            # Cooperative wait; under gevent this yields to other greenlets.
            time.sleep(0.05)
        return False

    def _release_inflight_slot() -> None:
        nonlocal inflight_count
        if inflight_semaphore is None:
            return
        try:
            with inflight_count_lock:
                inflight_count = max(0, inflight_count - 1)
            inflight_semaphore.release()
        except ValueError:
            # Guard against accidental double release.
            pass

    def _busy_payload() -> Dict[str, str]:
        limit = app.config.get("MAX_INFLIGHT_REQUESTS", 0)
        wait_s = app.config.get("INFLIGHT_QUEUE_WAIT_SECONDS", 0)
        return {
            "error": (
                "Systemet er midlertidigt overbelastet. "
                f"For mange samtidige forespørgsler (grænse: {limit}). "
                f"Forespørgslen kom ikke frem fra køen inden for {int(wait_s)} sekunder. Prøv igen."
            )
        }

    def _request_nonstream_completion(
        messages: List[Dict[str, str]],
        files: List[Dict[str, str]],
        user_prompt: str,
        enable_web_search: bool = False,
    ) -> str:
        payload = {
            "model": app.config["MODEL_NAME"],
            "messages": messages,
            "stream": False,
            **_generation_options(user_prompt),
        }
        if files:
            payload["files"] = files
        if enable_web_search:
            payload["features"] = {"web_search": True}
            payload["search_web"] = True

        r = _post_chat_with_failover(payload, stream=False)
        r.raise_for_status()
        resp = r.json()
        first_choice = (resp.get("choices") or [{}])[0] or {}
        msg_content = (first_choice.get("message", {}) or {}).get("content")
        full_response = _extract_content_text(msg_content)
        if not full_response:
            alt = first_choice.get("text")
            if isinstance(alt, str):
                full_response = alt
        return full_response

    def _complete_news_with_verified_links(
        messages: List[Dict[str, str]],
        files: List[Dict[str, str]],
        user_prompt: str,
    ) -> str:
        """
        Produce a news answer with best-effort verified links.
        If no valid links are found, performs one extra repair pass.
        """
        answer = _request_nonstream_completion(messages, files, user_prompt, enable_web_search=True)
        live: List[str] = []

        # Try to get at least one reachable link through up to 3 repair rounds.
        for _ in range(3):
            if _has_disallowed_news_content(answer):
                repaired_messages = list(messages)
                repaired_messages.append(
                    {
                        "role": "system",
                        "content": (
                            "Dit forrige svar var ugyldigt (simuleret/forældet år). "
                            "Svar kun med verificerbare, aktuelle kilder og uden simulering."
                        ),
                    }
                )
                answer = _request_nonstream_completion(repaired_messages, files, user_prompt, enable_web_search=True)
                continue
            urls = _extract_urls(answer)
            live = _reachable_urls(urls)
            if live:
                break
            repaired_messages = list(messages)
            repaired_messages.append(
                {
                    "role": "system",
                    "content": (
                        "Dit forrige svar havde ingen brugbare links eller links var utilgængelige. "
                        "Omskriv svaret med mindst 2 konkrete, fungerende URL'er (HTTP 200-399), hver med dato. "
                        "Ingen placeholders og ingen opdigtede links."
                    ),
                }
            )
            answer = _request_nonstream_completion(repaired_messages, files, user_prompt, enable_web_search=True)

        # Hard rule: no verified links => no news answer.
        if not live:
            return (
                "Jeg kan ikke give et nyhedssvar, fordi jeg ikke kunne verificere nogen fungerende kildelinks. "
                "Prøv igen med et mere specifikt emne eller et parti, så jeg kan finde verificerbare kilder."
            )

        # Rewrite answer to only use verified links.
        rewrite_messages = list(messages)
        rewrite_messages.append(
            {
                "role": "system",
                "content": (
                    "Omskriv nyhedssvaret, men brug KUN følgende verificerede links som kilder:\n"
                    + "\n".join(f"- {u}" for u in live)
                    + "\nDu må IKKE bruge andre links. "
                    "Hvis noget ikke kan underbygges af disse links, skal du skrive at det ikke kunne verificeres."
                ),
            }
        )
        rewritten = _request_nonstream_completion(rewrite_messages, files, user_prompt, enable_web_search=True)
        if _has_disallowed_news_content(rewritten):
            return (
                "Jeg kan ikke give et sikkert nyhedssvar, fordi modellen returnerede simuleret eller forældet indhold. "
                "Verificerede links lige nu:\n"
                + "\n".join(f"- {u}" for u in live)
            )
        rewritten_urls = _extract_urls(rewritten)
        allowed = set(live)
        disallowed = [u for u in rewritten_urls if u not in allowed]

        # If model still injects unverified links, fail closed.
        if disallowed:
            return (
                "Jeg kan ikke give et sikkert nyhedssvar uden ugyldige kilder. "
                "Verificerede links lige nu:\n"
                + "\n".join(f"- {u}" for u in live)
            )

        if "Kilder (verificeret)" not in rewritten:
            rewritten = (
                rewritten.rstrip()
                + "\n\nKilder (verificeret):\n"
                + "\n".join(f"- {u}" for u in live)
            )
        return rewritten

    def _backend_error_payload(exc: Exception, resp: requests.Response | None = None) -> Dict[str, str]:
        # Keep user output compact; keep details in server logs.
        if isinstance(exc, requests.exceptions.ReadTimeout):
            return {
                "error": (
                    f"Backend timeout: modellen svarede ikke inden for {int(app.config['HTTP_TIMEOUT_READ'])} sekunder. "
                    "Prøv igen om lidt."
                )
            }
        if isinstance(exc, requests.exceptions.ConnectTimeout):
            return {"error": "Backend timeout under opkobling. Prøv igen om lidt."}
        if isinstance(exc, requests.exceptions.ConnectionError):
            return {"error": "Backend utilgængelig (forbindelsesfejl). Prøv igen om lidt."}
        if resp is not None:
            return {"error": f"Backend HTTP-fejl {resp.status_code}."}
        return {"error": "Backend utilgængelig eller langsom. Prøv igen."}

    @app.route("/", methods=["GET"])
    def index():
        # Start a fresh conversation id whenever chat UI is opened.
        session_id = _new_session_id()
        _ensure_conversation(session_id)

        resp = make_response(
            render_template(
                "index.html",
                public_page_url=_public_url("/"),
                social_preview_url=_public_url("/social-preview.png"),
            )
        )
        resp.set_cookie(
            "session_id",
            session_id,
            max_age=60 * 60 * 24 * 30,
            httponly=True,
            samesite="Lax",
        )
        return resp

    @app.route("/api/chat", methods=["POST"])
    def chat():
        """Non-streaming fallback endpoint (returns JSON when done)."""
        data = request.get_json(silent=True) or {}
        user_prompt = (data.get("prompt") or "").strip()
        if not user_prompt:
            return jsonify({"error": "No prompt provided."}), 400

        session_id = request.cookies.get("session_id") or _new_session_id()
        _ensure_conversation(session_id)
        _record_request_start("/api/chat", session_id=session_id)
        _record_session_state(session_id, "active")

        if not _is_in_political_scope(user_prompt, session_id):
            blocked = _out_of_scope_reply()
            interaction_id = _log_interaction(
                session_id, user_prompt, blocked, [], [],
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": blocked, "selected_kbs": [], "interaction_id": interaction_id})

        ok, msg = _apply_rate_limit(session_id)
        if not ok:
            return jsonify({"error": msg}), 429

        # Store user turn
        _append_turn(session_id, "user", user_prompt)
        _register_user_preference_signal(session_id, user_prompt)
        model_prompt = _expand_user_prompt_for_model(session_id, user_prompt)

        onboarding_reply = _local_onboarding_response(session_id, user_prompt)
        if onboarding_reply:
            _append_turn(session_id, "assistant", onboarding_reply)
            interaction_id = _log_interaction(
                session_id, user_prompt, onboarding_reply, [], [],
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": onboarding_reply, "selected_kbs": [], "interaction_id": interaction_id})

        clarification_reply = _local_bound_clarification_response(session_id)
        if clarification_reply:
            _append_turn(session_id, "assistant", clarification_reply)
            interaction_id = _log_interaction(
                session_id, user_prompt, clarification_reply, [], [],
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": clarification_reply, "selected_kbs": [], "interaction_id": interaction_id})

        files, selected_kbs = _selected_collections(model_prompt, session_id)
        direct_party_list = _direct_party_list_answer(model_prompt)
        if direct_party_list:
            direct_party_list = _ensure_followup_question(direct_party_list, user_prompt, session_id)
            _append_turn(session_id, "assistant", direct_party_list)
            selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
            interaction_id = _log_interaction(
                session_id, user_prompt, direct_party_list, selected_kbs, selected_collection_ids,
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": direct_party_list, "selected_kbs": selected_kbs, "interaction_id": interaction_id})

        direct_party_presence = _direct_party_presence_answer(model_prompt)
        if direct_party_presence:
            direct_party_presence = _ensure_followup_question(direct_party_presence, user_prompt, session_id)
            _append_turn(session_id, "assistant", direct_party_presence)
            selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
            interaction_id = _log_interaction(
                session_id, user_prompt, direct_party_presence, selected_kbs, selected_collection_ids,
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": direct_party_presence, "selected_kbs": selected_kbs, "interaction_id": interaction_id})

        direct_election = _direct_election_answer(model_prompt)
        if direct_election:
            direct_election = _ensure_followup_question(direct_election, user_prompt, session_id)
            _append_turn(session_id, "assistant", direct_election)
            selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
            interaction_id = _log_interaction(
                session_id, user_prompt, direct_election, selected_kbs, selected_collection_ids,
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": direct_election, "selected_kbs": selected_kbs, "interaction_id": interaction_id})

        direct_answer = _direct_party_facts_answer(model_prompt, selected_kbs, session_id)
        if direct_answer:
            direct_answer = _ensure_followup_question(direct_answer, user_prompt, session_id)
            _append_turn(session_id, "assistant", direct_answer)
            selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
            interaction_id = _log_interaction(
                session_id, user_prompt, direct_answer, selected_kbs, selected_collection_ids,
                ttft_ms=0, latency_ms=0, ttft_source="local_rule"
            )
            return jsonify({"output": direct_answer, "selected_kbs": selected_kbs, "interaction_id": interaction_id})

        messages = _build_messages(session_id, model_prompt, files, selected_kbs)
        slot_acquired = _try_acquire_inflight_slot()
        if not slot_acquired:
            _record_request_metric(
                endpoint="/api/chat",
                outcome="busy",
                session_id=session_id,
                status_code=429,
            )
            return jsonify(_busy_payload()), 429

        try:
            t0 = time.perf_counter()
            if app.config.get("ENABLE_WEB_SEARCH", False) and _is_news_turn(session_id, user_prompt):
                full_response = _complete_news_with_verified_links(messages, files, user_prompt)
            else:
                full_response = _request_nonstream_completion(messages, files, user_prompt)
            latency_ms = int((time.perf_counter() - t0) * 1000)
        except requests.HTTPError as exc:
            resp = getattr(exc, "response", None)
            body = (resp.text[:800] if resp is not None and resp.text else "")
            print(f"Backend HTTPError: {resp.status_code if resp else '??'} {body}")
            _record_request_metric(
                endpoint="/api/chat",
                outcome="backend_error",
                session_id=session_id,
                status_code=resp.status_code if resp is not None else 502,
            )
            return jsonify(_backend_error_payload(exc, resp)), 502
        except Exception as exc:
            print(f"Backend request failed: {repr(exc)}")
            _record_request_metric(
                endpoint="/api/chat",
                outcome="backend_error",
                session_id=session_id,
                status_code=502,
            )
            return jsonify(_backend_error_payload(exc, None)), 502
        finally:
            _release_inflight_slot()

        full_response = _remove_urls_from_text(full_response)
        full_response = _prepend_bound_followup_acknowledgment(full_response, session_id, user_prompt)
        full_response = _ensure_followup_question(full_response, user_prompt, session_id)

        # Store assistant turn and log
        _append_turn(session_id, "assistant", full_response)
        selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
        interaction_id = _log_interaction(
            session_id,
            user_prompt,
            full_response,
            selected_kbs,
            selected_collection_ids,
            ttft_ms=latency_ms,          # non-streaming endpoint: TTFT is estimated as full roundtrip
            latency_ms=latency_ms,
            ttft_source="estimated_nonstream",
        )

        return jsonify({"output": full_response, "selected_kbs": selected_kbs, "interaction_id": interaction_id})

    @app.route("/api/chat_stream", methods=["POST"])
    def chat_stream():
        """Streaming endpoint using SSE. Frontend reads tokens live."""
        data = request.get_json(silent=True) or {}
        user_prompt = (data.get("prompt") or "").strip()
        if not user_prompt:
            return jsonify({"error": "No prompt provided."}), 400

        session_id = request.cookies.get("session_id") or _new_session_id()
        _ensure_conversation(session_id)
        _record_request_start("/api/chat_stream", session_id=session_id)
        _record_session_state(session_id, "active")

        if not _is_in_political_scope(user_prompt, session_id):
            blocked = _out_of_scope_reply()

            def blocked_generate():
                yield f"data: {json.dumps({'token': blocked}, ensure_ascii=False)}\n\n"
                yield "data: [DONE]\n\n"
                _log_interaction(
                    session_id, user_prompt, blocked, [], [],
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )

            return Response(stream_with_context(blocked_generate()), mimetype="text/event-stream")

        ok, msg = _apply_rate_limit(session_id)
        if not ok:
            return jsonify({"error": msg}), 429

        # Store user turn
        _append_turn(session_id, "user", user_prompt)
        _register_user_preference_signal(session_id, user_prompt)
        model_prompt = _expand_user_prompt_for_model(session_id, user_prompt)

        onboarding_reply = _local_onboarding_response(session_id, user_prompt)
        if onboarding_reply:
            def onboarding_generate():
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                yield f"data: {json.dumps({'token': onboarding_reply}, ensure_ascii=False)}\n\n"
                _append_turn(session_id, "assistant", onboarding_reply)
                interaction_id = _log_interaction(
                    session_id, user_prompt, onboarding_reply, [], [],
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )
                yield f"data: {json.dumps({'done': True, 'selected_kbs': [], 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
            return Response(stream_with_context(onboarding_generate()), mimetype="text/event-stream")

        clarification_reply = _local_bound_clarification_response(session_id)
        if clarification_reply:
            def clarification_generate():
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                yield f"data: {json.dumps({'token': clarification_reply}, ensure_ascii=False)}\n\n"
                _append_turn(session_id, "assistant", clarification_reply)
                interaction_id = _log_interaction(
                    session_id, user_prompt, clarification_reply, [], [],
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )
                yield f"data: {json.dumps({'done': True, 'selected_kbs': [], 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
            return Response(stream_with_context(clarification_generate()), mimetype="text/event-stream")

        files, selected_kbs = _selected_collections(model_prompt, session_id)
        direct_party_list = _direct_party_list_answer(model_prompt)
        if direct_party_list:
            def party_list_generate():
                reply = _ensure_followup_question(direct_party_list, user_prompt, session_id)
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                yield f"data: {json.dumps({'token': reply}, ensure_ascii=False)}\n\n"
                _append_turn(session_id, "assistant", reply)
                selected_collection_ids = [f.get('id') for f in files if isinstance(f, dict) and f.get('id')]
                interaction_id = _log_interaction(
                    session_id, user_prompt, reply, selected_kbs, selected_collection_ids,
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )
                yield f"data: {json.dumps({'done': True, 'selected_kbs': selected_kbs, 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
            return Response(stream_with_context(party_list_generate()), mimetype="text/event-stream")

        direct_party_presence = _direct_party_presence_answer(model_prompt)
        if direct_party_presence:
            def party_presence_generate():
                reply = _ensure_followup_question(direct_party_presence, user_prompt, session_id)
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                yield f"data: {json.dumps({'token': reply}, ensure_ascii=False)}\n\n"
                _append_turn(session_id, "assistant", reply)
                selected_collection_ids = [f.get('id') for f in files if isinstance(f, dict) and f.get('id')]
                interaction_id = _log_interaction(
                    session_id, user_prompt, reply, selected_kbs, selected_collection_ids,
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )
                yield f"data: {json.dumps({'done': True, 'selected_kbs': selected_kbs, 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
            return Response(stream_with_context(party_presence_generate()), mimetype="text/event-stream")

        direct_election = _direct_election_answer(model_prompt)
        if direct_election:
            def election_generate():
                reply = _ensure_followup_question(direct_election, user_prompt, session_id)
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                yield f"data: {json.dumps({'token': reply}, ensure_ascii=False)}\n\n"
                _append_turn(session_id, "assistant", reply)
                selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
                interaction_id = _log_interaction(
                    session_id, user_prompt, reply, selected_kbs, selected_collection_ids,
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )
                yield f"data: {json.dumps({'done': True, 'selected_kbs': selected_kbs, 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
            return Response(stream_with_context(election_generate()), mimetype="text/event-stream")

        direct_answer = _direct_party_facts_answer(model_prompt, selected_kbs, session_id)
        if direct_answer:
            def direct_generate():
                reply = _ensure_followup_question(direct_answer, user_prompt, session_id)
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                yield f"data: {json.dumps({'token': reply}, ensure_ascii=False)}\n\n"
                _append_turn(session_id, "assistant", reply)
                selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
                interaction_id = _log_interaction(
                    session_id, user_prompt, reply, selected_kbs, selected_collection_ids,
                    ttft_ms=0, latency_ms=0, ttft_source="local_rule"
                )
                yield f"data: {json.dumps({'done': True, 'selected_kbs': selected_kbs, 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
            return Response(stream_with_context(direct_generate()), mimetype="text/event-stream")

        messages = _build_messages(session_id, model_prompt, files, selected_kbs)
        slot_acquired = _try_acquire_inflight_slot()
        if not slot_acquired:
            _record_request_metric(
                endpoint="/api/chat_stream",
                outcome="busy",
                session_id=session_id,
                status_code=429,
            )
            return jsonify(_busy_payload()), 429

        # Optional web-search path (kept for future use, default disabled).
        if app.config.get("ENABLE_WEB_SEARCH", False) and _is_news_turn(session_id, user_prompt):
            def news_generate():
                full_response = ""
                logged_response = False
                interaction_id = ""
                t0 = time.perf_counter()
                first_token_ms: int | None = None
                yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"
                try:
                    full_response = _complete_news_with_verified_links(messages, files, user_prompt)
                    full_response = _remove_urls_from_text(full_response)
                    full_response = _prepend_bound_followup_acknowledgment(full_response, session_id, user_prompt)
                    full_response = _ensure_followup_question(full_response, user_prompt, session_id)
                    first_token_ms = int((time.perf_counter() - t0) * 1000)
                    yield f"data: {json.dumps({'token': full_response}, ensure_ascii=False)}\n\n"
                    if full_response:
                        _append_turn(session_id, "assistant", full_response)
                        selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
                        latency_ms = int((time.perf_counter() - t0) * 1000)
                        interaction_id = _log_interaction(
                            session_id, user_prompt, full_response, selected_kbs, selected_collection_ids,
                            ttft_ms=first_token_ms, latency_ms=latency_ms, ttft_source="stream"
                        )
                        logged_response = True
                    yield f"data: {json.dumps({'done': True, 'selected_kbs': selected_kbs, 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"
                except requests.HTTPError as exc:
                    resp = getattr(exc, "response", None)
                    status_code = resp.status_code if resp is not None else "??"
                    body = (resp.text[:800] if resp is not None and resp.text else "")
                    print(f"Backend news HTTPError: {status_code} {body}")
                    yield f"data: {json.dumps(_backend_error_payload(exc, resp), ensure_ascii=False)}\n\n"
                except Exception as exc:
                    print(f"Backend news request failed: {repr(exc)}")
                    yield f"data: {json.dumps(_backend_error_payload(exc, None), ensure_ascii=False)}\n\n"
                finally:
                    if full_response and not logged_response:
                        _append_turn(session_id, "assistant", full_response)
                        selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
                        latency_ms = int((time.perf_counter() - t0) * 1000)
                        _log_interaction(
                            session_id, user_prompt, full_response, selected_kbs, selected_collection_ids,
                            ttft_ms=first_token_ms, latency_ms=latency_ms, ttft_source="stream"
                        )
                    _release_inflight_slot()

            return Response(stream_with_context(news_generate()), mimetype="text/event-stream")

        payload = {
            "model": app.config["MODEL_NAME"],
            "messages": messages,
            "stream": True,
            **_generation_options(user_prompt),
        }
        if files:
            payload["files"] = files

        def generate():
            full_response = ""
            streamed_anything = False
            logged_response = False
            interaction_id = ""
            backend_url = ""
            t0 = time.perf_counter()
            first_token_ms: int | None = None
            buffer_vote_flow = _is_vote_advice_query(user_prompt) or _is_vote_advice_followup(session_id, user_prompt)
            # Let the client know we're connected (helps debugging perceived hangs)
            yield f"data: {json.dumps({'status': 'connecting'}, ensure_ascii=False)}\n\n"

            try:
                r = _post_chat_with_failover(payload, stream=True)
                backend_url = getattr(r, "ddc_backend_url", "")
                r.raise_for_status()

                # Handle OpenAI-style SSE and plain JSONL-style streams.
                for raw in r.iter_lines(decode_unicode=True):
                    if not raw:
                        continue

                    line = raw.strip()
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                    else:
                        data_str = line
                    if data_str == "[DONE]":
                        break

                    try:
                        evt = json.loads(data_str)
                    except json.JSONDecodeError:
                        # Some backends may stream raw token lines.
                        token = data_str
                        if token:
                            streamed_anything = True
                            full_response += token
                            safe_token = _remove_urls_from_text(token)
                            if safe_token and not buffer_vote_flow:
                                if first_token_ms is None:
                                    first_token_ms = int((time.perf_counter() - t0) * 1000)
                                yield f"data: {json.dumps({'token': safe_token}, ensure_ascii=False)}\n\n"
                        continue

                    token = ""
                    choices = evt.get("choices") or []
                    if choices:
                        choice0 = choices[0] or {}
                        delta = choice0.get("delta") or {}
                        token = _extract_content_text(delta.get("content"))
                        if not token:
                            # Some backends stream plain text under different keys
                            alt = choice0.get("text")
                            if isinstance(alt, str):
                                token = alt
                        if not token:
                            msg = (choice0.get("message") or {}).get("content")
                            token = _extract_content_text(msg)
                    if not token:
                        # Common non-OpenAI keys seen in some proxies/backends
                        for k in ("token", "text", "response", "content"):
                            v = evt.get(k)
                            if isinstance(v, str) and v:
                                token = v
                                break

                    if token:
                        streamed_anything = True
                        full_response += token
                        safe_token = _remove_urls_from_text(token)
                        if safe_token and not buffer_vote_flow:
                            if first_token_ms is None:
                                first_token_ms = int((time.perf_counter() - t0) * 1000)
                            yield f"data: {json.dumps({'token': safe_token}, ensure_ascii=False)}\n\n"

                # Safety net: if stream succeeded but produced no tokens, do one non-stream call.
                if not streamed_anything and not full_response:
                    fallback_payload = {
                        "model": app.config["MODEL_NAME"],
                        "messages": messages,
                        "stream": False,
                        **_generation_options(user_prompt),
                    }
                    if files:
                        fallback_payload["files"] = files
                    rr = _post_chat_with_failover(fallback_payload, stream=False)
                    rr.raise_for_status()
                    fallback_json = rr.json()
                    choice0 = (fallback_json.get("choices") or [{}])[0] or {}
                    msg_content = (choice0.get("message", {}) or {}).get("content")
                    fallback_text = _extract_content_text(msg_content)
                    if not fallback_text:
                        alt = choice0.get("text")
                        if isinstance(alt, str):
                            fallback_text = alt
                    if fallback_text:
                        full_response += fallback_text
                        safe_fallback = _remove_urls_from_text(fallback_text)
                        if safe_fallback and not buffer_vote_flow:
                            if first_token_ms is None:
                                first_token_ms = int((time.perf_counter() - t0) * 1000)
                            yield f"data: {json.dumps({'token': safe_fallback}, ensure_ascii=False)}\n\n"

                full_response = _remove_urls_from_text(full_response)
                full_response = _prepend_bound_followup_acknowledgment(full_response, session_id, user_prompt)
                with_followup = _ensure_followup_question(full_response, user_prompt, session_id)
                if buffer_vote_flow:
                    if with_followup:
                        if first_token_ms is None:
                            first_token_ms = int((time.perf_counter() - t0) * 1000)
                        yield f"data: {json.dumps({'token': with_followup}, ensure_ascii=False)}\n\n"
                elif len(with_followup) > len(full_response):
                    suffix = with_followup[len(full_response):]
                    if suffix:
                        if first_token_ms is None:
                            first_token_ms = int((time.perf_counter() - t0) * 1000)
                        yield f"data: {json.dumps({'token': suffix}, ensure_ascii=False)}\n\n"
                full_response = with_followup
                if full_response:
                    _append_turn(session_id, "assistant", full_response)
                    selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
                    latency_ms = int((time.perf_counter() - t0) * 1000)
                    interaction_id = _log_interaction(
                        session_id, user_prompt, full_response, selected_kbs, selected_collection_ids,
                        ttft_ms=first_token_ms, latency_ms=latency_ms, ttft_source="stream",
                        backend_url=backend_url,
                    )
                    logged_response = True

                # Final event includes KB list for debugging
                yield f"data: {json.dumps({'done': True, 'selected_kbs': selected_kbs, 'interaction_id': interaction_id}, ensure_ascii=False)}\n\n"

            except requests.HTTPError as exc:
                resp = getattr(exc, "response", None)
                status_code = resp.status_code if resp is not None else "??"
                body = (resp.text[:800] if resp is not None and resp.text else "")
                print(f"Backend stream HTTPError: {status_code} {body}")
                _record_request_metric(
                    endpoint="/api/chat_stream",
                    outcome="backend_error",
                    session_id=session_id,
                    status_code=resp.status_code if resp is not None else 502,
                    backend_url=backend_url,
                )
                yield f"data: {json.dumps(_backend_error_payload(exc, resp), ensure_ascii=False)}\n\n"
            except Exception as exc:
                print(f"Backend stream failed: {repr(exc)}")
                _record_request_metric(
                    endpoint="/api/chat_stream",
                    outcome="backend_error",
                    session_id=session_id,
                    status_code=502,
                    backend_url=backend_url,
                )
                yield f"data: {json.dumps(_backend_error_payload(exc, None), ensure_ascii=False)}\n\n"
            finally:
                if full_response and not logged_response:
                    _append_turn(session_id, "assistant", full_response)
                    selected_collection_ids = [f.get("id") for f in files if isinstance(f, dict) and f.get("id")]
                    latency_ms = int((time.perf_counter() - t0) * 1000)
                    _log_interaction(
                        session_id, user_prompt, full_response, selected_kbs, selected_collection_ids,
                        ttft_ms=first_token_ms, latency_ms=latency_ms, ttft_source="stream",
                        backend_url=backend_url,
                    )
                _release_inflight_slot()

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/api/reset", methods=["POST"])
    def reset():
        """Reset conversation and rotate to a new conversation id."""
        session_id = request.cookies.get("session_id")
        old_deletion_code = session_deletion_codes.get(session_id or "", "")
        if session_id and session_id in conversations:
            _record_session_state(session_id, "reset")
            _clear_session_state(session_id)

        new_session_id = _new_session_id()
        _ensure_conversation(new_session_id)
        if old_deletion_code:
            session_deletion_codes[new_session_id] = old_deletion_code
        resp = jsonify({"ok": True, "session_id": new_session_id})
        resp.set_cookie(
            "session_id",
            new_session_id,
            max_age=60 * 60 * 24 * 30,
            httponly=True,
            samesite="Lax",
        )
        return resp

    @app.route("/api/withdraw_consent", methods=["POST"])
    def withdraw_consent():
        """
        Withdraw consent and delete all data tied to current session_id.
        """
        session_id = request.cookies.get("session_id") or ""
        if not session_id:
            return jsonify({"ok": False, "error": "No active session."}), 400

        # Keep minimal audit intent event before deletion.
        _log_session_event(session_id, "consent_withdrawn", {"requested": True})
        _record_session_state(session_id, "withdrawn")
        deletion = _delete_session_data(session_id)

        new_session_id = _new_session_id()
        _ensure_conversation(new_session_id)
        resp = jsonify(
            {
                "ok": True,
                "deleted_session_id": session_id,
                "deleted_log_rows": deletion.get("deleted_log_rows", 0),
                "session_id": new_session_id,
            }
        )
        resp.set_cookie(
            "session_id",
            new_session_id,
            max_age=60 * 60 * 24 * 30,
            httponly=True,
            samesite="Lax",
        )
        return resp

    @app.route("/api/feedback", methods=["POST"])
    def feedback():
        data = request.get_json(silent=True) or {}
        rating = (data.get("rating") or "").strip().lower()
        if rating not in ("up", "down"):
            return jsonify({"error": "Invalid rating."}), 400

        interaction_id = (data.get("interaction_id") or data.get("message_id") or "").strip()
        prompt = (data.get("prompt") or "").strip()
        response_text = (data.get("response") or "").strip()

        session_id = request.cookies.get("session_id") or _new_session_id()
        _log_feedback_event(
            session_id,
            rating=rating,
            interaction_id=interaction_id,
            prompt=prompt,
            response_text=response_text,
        )
        return jsonify({"ok": True})

    @app.route("/api/session_event", methods=["POST"])
    def session_event():
        data = request.get_json(silent=True) or {}
        event_type = (data.get("event_type") or "").strip().lower()
        if event_type not in ("briefing_ack", "debrief_submit", "debrief_skip", "debrief_link_click", "consent_withdrawn"):
            return jsonify({"error": "Invalid event_type."}), 400
        payload = data.get("payload")
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            return jsonify({"error": "payload must be an object."}), 400
        session_id = request.cookies.get("session_id") or _new_session_id()
        if event_type == "briefing_ack":
            code = _get_or_create_deletion_code(session_id)
            payload = dict(payload)
            payload["deletion_code"] = code
            payload["deletion_contact"] = "your-contact@example.org"
        _log_session_event(session_id, event_type, payload)
        if event_type == "briefing_ack":
            _record_session_state(session_id, "briefed")
        elif event_type in ("debrief_submit", "debrief_skip"):
            _record_session_state(session_id, "completed")
        elif event_type == "consent_withdrawn":
            _record_session_state(session_id, "withdrawn")
        if event_type == "briefing_ack":
            return jsonify({"ok": True, "deletion_code": payload.get("deletion_code", ""), "deletion_contact": payload.get("deletion_contact", "")})
        return jsonify({"ok": True})

    @app.route("/social-preview.png", methods=["GET"])
    def social_preview():
        width, height = 1200, 630
        img = Image.new("RGB", (width, height), "#f3f0ea")
        draw = ImageDraw.Draw(img)

        title_font = _load_preview_font(44, bold=True)
        subtitle_font = _load_preview_font(25, bold=False)
        card_title_font = _load_preview_font(28, bold=True)
        body_font = _load_preview_font(24, bold=False)
        small_font = _load_preview_font(20, bold=False)
        button_font = _load_preview_font(24, bold=True)

        draw.rounded_rectangle((34, 34, width - 34, height - 34), radius=28, fill="#fffdf9", outline="#d8d2ca", width=2)
        draw.text((70, 62), "AI ValgChat FV26", font=title_font, fill="#11213b")
        draw.text(
            (70, 118),
            "Eksperimentel AI-chat om dansk politik og folketingsvalget 2026",
            font=subtitle_font,
            fill="#3b4b63",
        )

        chat_x0, chat_y0, chat_x1, chat_y1 = 70, 180, width - 70, 535
        draw.rounded_rectangle((chat_x0, chat_y0, chat_x1, chat_y1), radius=24, fill="#ffffff", outline="#d9dde3", width=2)
        title_bbox = draw.textbbox((0, 0), "AI ValgChat FV26", font=card_title_font)
        title_w = title_bbox[2] - title_bbox[0]
        draw.text((chat_x0 + ((chat_x1 - chat_x0 - title_w) / 2), chat_y0 + 24), "AI ValgChat FV26", font=card_title_font, fill="#13233d")

        bubble_x0, bubble_y0, bubble_x1, bubble_y1 = chat_x0 + 24, chat_y0 + 98, chat_x1 - 28, chat_y0 + 246
        draw.rounded_rectangle((bubble_x0, bubble_y0, bubble_x1, bubble_y1), radius=18, fill="#fdfefe", outline="#d8dde5", width=2)
        draw.text((bubble_x0 + 18, bubble_y0 + 14), "AI:", font=small_font, fill="#59697d")
        preview_lines = [
            "Fortæl hvilke emner du går mest op i,",
            "for eksempel klima, økonomi, velfærd",
            "eller immigration, så giver jeg et",
            "foreløbigt politisk match og sammenligninger.",
        ]
        y = bubble_y0 + 48
        for line in preview_lines:
            draw.text((bubble_x0 + 18, y), line, font=body_font, fill="#14253f")
            y += 30

        input_x0, input_y0, input_x1, input_y1 = chat_x0 + 24, chat_y1 - 108, chat_x1 - 166, chat_y1 - 32
        draw.rounded_rectangle((input_x0, input_y0, input_x1, input_y1), radius=18, fill="#f7f8fa", outline="#d9dde3", width=2)
        draw.text((input_x0 + 18, input_y0 + 22), "Stil dit spørgsmål ...", font=body_font, fill="#7a8798")

        send_x0, send_y0, send_x1, send_y1 = chat_x1 - 138, chat_y1 - 108, chat_x1 - 24, chat_y1 - 32
        draw.rounded_rectangle((send_x0, send_y0, send_x1, send_y1), radius=18, fill="#127a72", outline="#127a72", width=2)
        send_bbox = draw.textbbox((0, 0), "Send", font=button_font)
        send_w = send_bbox[2] - send_bbox[0]
        send_h = send_bbox[3] - send_bbox[1]
        draw.text(
            (send_x0 + ((send_x1 - send_x0 - send_w) / 2), send_y0 + ((send_y1 - send_y0 - send_h) / 2) - 2),
            "Send",
            font=button_font,
            fill="#ffffff",
        )

        warning_x0, warning_y0, warning_x1, warning_y1 = 70, height - 118, width - 70, height - 64
        draw.rounded_rectangle((warning_x0, warning_y0, warning_x1, warning_y1), radius=18, fill="#fff1ef", outline="#f0b7b1", width=2)
        draw.text(
            (warning_x0 + 18, warning_y0 + 15),
            "Eksperimentelt forskningsværktøj. Svar kan være upræcise eller forkerte.",
            font=small_font,
            fill="#b6332b",
        )

        footer = "SDU / Digital Democracy Centre"
        footer_bbox = draw.textbbox((0, 0), footer, font=small_font)
        footer_w = footer_bbox[2] - footer_bbox[0]
        draw.text((width - 70 - footer_w, height - 52), footer, font=small_font, fill="#5d6a79")

        buf = BytesIO()
        img.save(buf, format="PNG", optimize=True)
        buf.seek(0)
        return send_file(buf, mimetype="image/png", max_age=3600)

    @app.route("/api/admin/metrics", methods=["GET"])
    def admin_metrics():
        if not _require_admin_metrics_auth():
            return jsonify({"error": "Unauthorized"}), 401
        try:
            window_seconds = max(30, min(3600, int(request.args.get("window", "300"))))
        except Exception:
            window_seconds = 300
        return jsonify(_build_admin_metrics_snapshot(window_seconds))

    # Startup prints (safe, no secrets)
    print("CHAT_API_URL =", app.config["CHAT_API_URL"])
    print("CHAT_API_URLS =", app.config.get("CHAT_API_URLS"))
    print("MODEL_NAME   =", app.config["MODEL_NAME"])
    print("TLS_VERIFY   =", app.config["TLS_VERIFY"])
    print("HAS_API_KEY  =", bool(app.config["WEBUI_API_KEY"]))
    print("COLLECTIONS  =", len(app.config.get("COLLECTION_ID_MAP", {}) or {}))
    print("MAPPED_PARTIES =", len(app.config.get("PARTY_CONTEXT_MAP", {}) or {}))
    print("MP_NAME_MAPPINGS =", len(app.config.get("MP_NAME_PARTY_MAP", {}) or {}))
    print("PARTY_FACTS =", len(app.config.get("PARTY_FACT_MAP", {}) or {}))
    print("PARTY_FACTS_CANONICAL =", len(app.config.get("PARTY_FACTS_CANONICAL", {}) or {}))
    print("MAX_COLLECTIONS_PER_REQUEST =", app.config.get("MAX_COLLECTIONS_PER_REQUEST"))
    print("SHORTLIST_SIZE =", app.config.get("SHORTLIST_SIZE"))
    print("BROAD_QUERY_MAX_COLLECTIONS =", app.config.get("BROAD_QUERY_MAX_COLLECTIONS"))
    print("FALLBACK_TO_ALL_CONTEXTS_FOR_BROAD =", app.config.get("FALLBACK_TO_ALL_CONTEXTS_FOR_BROAD"))
    print("ENABLE_WEB_SEARCH =", app.config.get("ENABLE_WEB_SEARCH"))
    print("BACKEND_FAILOVER_ENABLED =", app.config.get("BACKEND_FAILOVER_ENABLED"))
    print("BACKEND_FAILOVER_ON_401 =", app.config.get("BACKEND_FAILOVER_ON_401"))
    print("ADAPTIVE_SHORT_ANSWERS =", app.config.get("ADAPTIVE_SHORT_ANSWERS"))
    print("ENABLE_FOLLOWUP_QUESTION =", app.config.get("ENABLE_FOLLOWUP_QUESTION"))
    print("METRICS_LOG_FILE =", app.config.get("METRICS_LOG_FILE"))
    print("HAS_ADMIN_METRICS_TOKEN =", bool(app.config.get("ADMIN_METRICS_TOKEN")))

    return app


if __name__ == "__main__":
    flask_app = create_app()
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
