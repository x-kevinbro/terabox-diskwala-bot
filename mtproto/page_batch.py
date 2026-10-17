"""Sequential orchestration, separate from Telegram so it can be tested offline."""
from __future__ import annotations

from dataclasses import dataclass

MAX_FILES = 100
MAX_PENDING_BATCHES = 4


@dataclass
class BatchStats:
    links: int = 0
    link_failures: int = 0
    sent: int = 0
    skipped: int = 0
    failed: int = 0
    limited: bool = False


async def run_batch(links, resolver, delivery, progress, stats: BatchStats | None = None) -> BatchStats:
    stats = stats if stats is not None else BatchStats()
    attempted_files = 0
    for number, url in enumerate(links, 1):
        if attempted_files >= MAX_FILES:
            stats.limited = True
            break
        await progress(number, len(links), stats)
        stats.links += 1
        try:
            result = await resolver(url)
            files = [(index, item) for index, item in enumerate(f for f in result.files if not f.is_dir)
                     if item.dlink]
            if not files:
                stats.link_failures += 1
                continue
        except Exception:
            stats.link_failures += 1
            continue
        for index, item in files:
            if attempted_files >= MAX_FILES:
                stats.limited = True
                break
            attempted_files += 1
            try:
                outcome = await delivery(result, item, index)
            except Exception:
                outcome = "failed"
            if outcome == "sent":
                stats.sent += 1
            elif outcome == "skipped":
                stats.skipped += 1
            else:
                stats.failed += 1
    return stats
