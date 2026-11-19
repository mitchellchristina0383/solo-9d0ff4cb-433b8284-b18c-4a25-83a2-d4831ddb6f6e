"""FastAPI web control surface and real NTP probe client."""

from __future__ import annotations

import datetime as dt
import os
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from . import upstream
from .clock import ClockState, EventLog, HistoryStore
from .config import Settings, load_settings
from .ntp_server import NtpServer
from .sampler import Sampler
from .upstream import UpstreamConfig


class SourceToggle(BaseModel):
    enabled: bool


class ProbeRequest(BaseModel):
    source_id: str | None = None
    timeout: float = Field(default=2.0, ge=0.1, le=10.0)


class PauseRequest(BaseModel):
    paused: bool


class AppState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.events = EventLog()
        self.history = HistoryStore(settings.data_path)
        self.clock = ClockState(
            holdover_seconds=settings.holdover_seconds,
            fresh_seconds=settings.fresh_seconds,
            rollback_tolerance=settings.rollback_tolerance,
            events=self.events,
            history=self.history,
        )
        self.sampler = Sampler(
            settings.upstreams,
            self.clock,
            interval=settings.sample_interval,
            timeout=settings.sample_timeout,
            events=self.events,
        )
        self.server = NtpServer(
            self.clock,
            host=settings.ntp_host,
            port=settings.ntp_port,
            events=self.events,
        )

    def start(self) -> None:
        self.server.start()
        self.sampler.start()

    def stop(self) -> None:
        self.sampler.stop()
        self.server.stop()


def _iso(value: float | None) -> str | None:
    if value is None:
        return None
    return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).isoformat(timespec="milliseconds")


def create_app(settings: Settings | None = None, state: AppState | None = None) -> FastAPI:
    settings = settings or load_settings()
    app_state = state or AppState(settings)
    app_state.events.add(
        "startup",
        "upstreams: "
        + ", ".join(f"{u.id}={u.host}:{u.port}" for u in app_state.settings.upstreams),
    )
    auto_start = state is None

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if auto_start:
            app_state.start()
        try:
            yield
        finally:
            if auto_start:
                app_state.stop()

    app = FastAPI(title="隔离网 NTP 服务", lifespan=lifespan)
    app.state.ntp = app_state
    templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

    def build_status() -> dict:
        st: AppState = app.state.ntp
        clock = st.clock.describe()
        sampler = st.sampler.describe()
        clock["iso_time"] = _iso(clock["unix_time"])
        clock["host_time"] = time.time()
        clock["host_delta"] = clock["unix_time"] - time.time()
        return {
            "clock": clock,
            "sampler": sampler,
            "server": st.server.describe(),
            "events": st.events.recent(30),
            "now_wall": time.time(),
            "now_iso": _iso(time.time()),
            "polling_paused": st.sampler.paused,
        }

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request, "index.html", {"status": build_status()}
        )

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True, "synced": app.state.ntp.clock.status().synced}

    @app.get("/api/status")
    def api_status() -> JSONResponse:
        return JSONResponse(build_status())

    @app.post("/api/sources/{source_id}/enabled")
    def api_toggle_source(source_id: str, body: SourceToggle) -> JSONResponse:
        st: AppState = app.state.ntp
        if not st.sampler.set_enabled(source_id, body.enabled):
            raise HTTPException(status_code=404, detail="unknown source")
        return JSONResponse(build_status())

    @app.post("/api/probe")
    def api_probe(body: ProbeRequest) -> JSONResponse:
        """Send a genuine NTP request over UDP and decode the reply.

        This is intentionally *not* routed through the sampler: the operator
        is asking "what does a client actually see from this upstream right
        now?", not "please resynchronise me".
        """
        st: AppState = app.state.ntp
        targets: list[UpstreamConfig]
        if body.source_id:
            targets = [s for s in st.settings.upstreams if s.id == body.source_id]
            if not targets:
                raise HTTPException(status_code=404, detail="unknown source")
        else:
            targets = list(st.settings.upstreams)

        results = []
        for cfg in targets:
            outcome = upstream.query(cfg, timeout=body.timeout)
            results.append(_outcome_dict(cfg, outcome))
        return JSONResponse({"probed_at": time.time(), "results": results})

    @app.post("/api/probe/server")
    def api_probe_self() -> JSONResponse:
        """Loopback NTP request against *our own* responder.

        Proves end-to-end that the daemon is answering with what the UI shows.
        """
        st: AppState = app.state.ntp
        port = st.server.bound_port or st.settings.ntp_port
        cfg = UpstreamConfig("self", "127.0.0.1", port, "本机服务")
        outcome = upstream.query(cfg, timeout=2.0)
        return JSONResponse({"probed_at": time.time(), "results": [_outcome_dict(cfg, outcome)]})

    @app.post("/api/sampler/paused")
    def api_pause(body: PauseRequest) -> JSONResponse:
        st: AppState = app.state.ntp
        st.sampler.set_paused(body.paused)
        return JSONResponse(build_status())

    @app.post("/api/refresh")
    def api_refresh() -> JSONResponse:
        st: AppState = app.state.ntp
        # Polling does blocking UDP I/O, so run it off the event loop thread.
        threading.Thread(target=st.sampler.poll_once, daemon=True).start()
        return JSONResponse(build_status())

    return app


def _outcome_dict(cfg: UpstreamConfig, outcome: upstream.QueryOutcome) -> dict:
    data = {
        "source_id": cfg.id,
        "address": f"{cfg.host}:{cfg.port}",
        "ok": outcome.ok,
        "error": outcome.error,
        "peer": outcome.peer,
        "stratum": outcome.stratum,
        "leap": outcome.leap,
        "root_delay": outcome.root_delay,
        "root_dispersion": outcome.root_dispersion,
        "rtt": outcome.rtt,
        "kiss_code": outcome.kiss_code,
    }
    if outcome.sample is not None:
        s = outcome.sample
        data["sample"] = {
            "offset": s.offset,
            "rtt": s.delay,
            "upstream_time": s.true_time,
            "upstream_iso": _iso(s.true_time),
            "our_time": s.local_wall,
            "delta_vs_our_time": s.true_time - s.local_wall,
        }
    return data


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    default_app = create_app()
    uvicorn.run(default_app, host="0.0.0.0", port=int(os.environ.get("HTTP_PORT", "8000")))
