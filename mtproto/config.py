"""Runtime configuration.

Every value comes from environment variables so nothing secret lives in the
repository. On Koyeb these are set as service environment variables; locally a
.env file is loaded if present (see .env.example).
"""
import os
from pathlib import Path


def _load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader. Real environment variables always win."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        return default


class Config:
    # --- Telegram MTProto credentials (my.telegram.org) ---
    api_id: int = _int("API_ID", 0)
    api_hash: str = os.getenv("API_HASH", "")
    bot_token: str = os.getenv("BOT_TOKEN", "")

    # Access control: public by default, optionally restrict to ALLOWED_USERS.
    public_bot: bool = str(os.getenv("PUBLIC_BOT", "true")).lower() in {"1", "true", "yes", "on"}
    allowed_users: set[int] = {
        int(part) for part in os.getenv("ALLOWED_USERS", "").replace(" ", "").split(",") if part.isdigit()
    }

    # MTProto allows 2000 MB per file for normal bots.
    max_file_mb: int = min(_int("MAX_FILE_MB", 2000), 2000)
    # Free Koyeb instances have small ephemeral disks; refuse anything bigger.
    max_disk_mb: int = _int("MAX_DISK_MB", 2600)
    download_dir: Path = Path(os.getenv("DOWNLOAD_DIR", "/tmp/downloads"))
    # 1 MiB chunks keep RSS flat regardless of file size.
    chunk_bytes: int = _int("CHUNK_BYTES", 1024 * 1024)
    # One transfer at a time protects the 512 MB instance from OOM/disk churn.
    max_concurrent_jobs: int = max(1, _int("MAX_CONCURRENT_JOBS", 1))
    request_timeout: int = _int("REQUEST_TIMEOUT_MS", 30000) // 1000
    progress_interval: float = float(_int("PROGRESS_INTERVAL_SEC", 6))
    port: int = _int("PORT", 8000)
    workdir: Path = Path(os.getenv("SESSION_DIR", "/tmp/session"))

    @property
    def max_bytes(self) -> int:
        return self.max_file_mb * 1024 * 1024

    def missing(self) -> list[str]:
        gaps = []
        if not self.api_id:
            gaps.append("API_ID")
        if not self.api_hash:
            gaps.append("API_HASH")
        if not self.bot_token:
            gaps.append("BOT_TOKEN")
        return gaps


config = Config()
