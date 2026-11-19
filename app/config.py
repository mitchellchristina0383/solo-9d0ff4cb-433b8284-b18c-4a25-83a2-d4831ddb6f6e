"""Environment-driven configuration."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from .upstream import UpstreamConfig


def _parse_upstreams(raw: str) -> list[UpstreamConfig]:
    """Accept either JSON or a simple ``id@host:port[:label]`` list."""
    raw = raw.strip()
    if not raw:
        return [
            UpstreamConfig("upstream-a", "upstream-a", 123, "上游 A"),
            UpstreamConfig("upstream-b", "upstream-b", 123, "上游 B"),
        ]
    if raw.startswith("["):
        data = json.loads(raw)
        return [
            UpstreamConfig(
                id=str(item["id"]),
                host=str(item["host"]),
                port=int(item.get("port", 123)),
                label=str(item.get("label", item["id"])),
            )
            for item in data
        ]
    result: list[UpstreamConfig] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        label = ""
        if "@" in chunk:
            ident, rest = chunk.split("@", 1)
        else:
            ident, rest = chunk, chunk
        parts = rest.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 123
        if len(parts) > 2:
            label = parts[2]
        result.append(UpstreamConfig(ident, host, port, label or ident))
    return result


@dataclass
class Settings:
    upstreams: list[UpstreamConfig]
    sample_interval: float
    sample_timeout: float
    holdover_seconds: float
    fresh_seconds: float
    rollback_tolerance: float
    ntp_host: str
    ntp_port: int
    http_port: int
    data_path: str
    probe_timeout: float


def load_settings() -> Settings:
    return Settings(
        upstreams=_parse_upstreams(os.environ.get("UPSTREAMS", "")),
        sample_interval=float(os.environ.get("SAMPLE_INTERVAL", "4")),
        sample_timeout=float(os.environ.get("SAMPLE_TIMEOUT", "2")),
        holdover_seconds=float(os.environ.get("HOLDOVER_SECONDS", "120")),
        fresh_seconds=float(os.environ.get("FRESH_SECONDS", "10")),
        rollback_tolerance=float(os.environ.get("ROLLBACK_TOLERANCE", "0.25")),
        ntp_host=os.environ.get("NTP_BIND", "0.0.0.0"),
        ntp_port=int(os.environ.get("NTP_PORT", "123")),
        http_port=int(os.environ.get("HTTP_PORT", "8000")),
        data_path=os.environ.get("DATA_PATH", "/data/history.json"),
        probe_timeout=float(os.environ.get("PROBE_TIMEOUT", "2")),
    )
