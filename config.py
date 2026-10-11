import os

from dotenv import load_dotenv

load_dotenv()

# ── awda public API ──────────────────────────────────────────────
AWDA_HOST = os.getenv("AWDA_HOST", "127.0.0.1")
AWDA_PORT = int(os.getenv("AWDA_PORT", "4098"))
AWDA_TOKEN = os.getenv("AWDA_TOKEN", "")
AWDA_CORS = os.getenv("AWDA_CORS", "*")

# ── opencode (same knobs as dex, so the copied bridge works as-is) ──
OPENCODE_PORT = int(os.getenv("OPENCODE_PORT", "4096"))
OPENCODE_MODEL_PROVIDER = os.getenv("OPENCODE_MODEL_PROVIDER", "opencode")
OPENCODE_MODEL_ID = os.getenv("OPENCODE_MODEL_ID", "space-bunny-free")
OPENCODE_MODEL_VARIANT = os.getenv("OPENCODE_MODEL_VARIANT", "low")
OPENCODE_TIMEOUT = int(os.getenv("OPENCODE_TIMEOUT", "420"))

# Agent used for API turns. Must exist in the opencode serve config root.
# Security: never accept file:// part URLs from API callers (a file part tells
# the *server* to read the URL — dex proved a deny-by-default agent still
# reads them). Callers send bytes (data: URLs); http(s) media is fetched here
# into capped bytes and re-encoded. Keep off unless serve + API are same-trust.
AWDA_ALLOW_FILE_URLS = os.getenv("AWDA_ALLOW_FILE_URLS", "").lower() in (
    "1", "true", "yes")
# "general" is built-in; dex-specific agents (deXC/dex-agent) only exist
# under ~/dex/.opencode.
AWDA_AGENT = os.getenv("AWDA_AGENT", "general")

# Models known-dead upstream — hidden from /v1/models, rejected fast.
# Probed live 2026-10-11 on opencode 1.18.9 via serve session API:
#   exo-free → "Model exo-free has been deprecated."
#   ling-3.0-flash-fin-free → "Upstream request failed: Model is unavailable."
DEAD_MODELS = set(
    m.strip()
    for m in os.getenv(
        "AWDA_DEAD_MODELS", "exo-free,ling-3.0-flash-fin-free"
    ).split(",")
    if m.strip()
)

# ── tiers (compat with dex bridge) ───────────────────────────────
DEFAULT_TIER = os.getenv("DEX_DEFAULT_TIER", "chat")
