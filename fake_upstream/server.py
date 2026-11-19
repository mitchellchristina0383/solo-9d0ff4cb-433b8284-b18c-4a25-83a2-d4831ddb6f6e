#!/usr/bin/env python3
"""Local, controllable NTPv4 upstream used by the Compose demo.

Two instances run as separate containers.  Each has an HTTP side-channel so
the operator can make it *look* wrong in the ways a real deployment cares
about:

  GET /                     status page
  POST /api/config          {"offset":1.5,"enabled":true,"stratum":2,
                             "lie":false,"spoof_origin":false,
                             "drop_responses":false}
  POST /api/step            {"delta": -3600}  (sudden rollback)

The NTP side is a real NTPv4 responder: it echoes the client transmit
timestamp in origin, fills receive/transmit, and is honest about its own
stratum/leap/refid.  ``lie`` raises the *reported* stratum to 16 / LI 3 so
tests can prove the daemon checks sync state rather than believing a reply
just because it arrived.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import struct
import sys
import threading
import time
from dataclasses import asdict, dataclass

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import ntp_proto  # noqa: E402


@dataclass
class Config:
    offset: float = 0.0
    enabled: bool = True
    stratum: int = 2
    lie: bool = False            # report LI=3 / stratum 16 while pretending to answer
    spoof_origin: bool = False   # echo a wrong origin timestamp
    drop_responses: bool = False
    refid: str = "UPST"


class FakeUpstream:
    def __init__(self, name: str, ntp_port: int, http_port: int) -> None:
        self.name = name
        self.ntp_port = ntp_port
        self.http_port = http_port
        self.cfg = Config(refid=name.upper()[:4])
        self.lock = threading.Lock()
        self.queries = 0
        self.served = 0
        self.dropped = 0
        self.started = time.time()
        self._stop = threading.Event()
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None

    # ---------------------------------------------------------------- NTP
    def current_time(self) -> float:
        with self.lock:
            return time.time() + self.cfg.offset

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.ntp_port))
        sock.settimeout(0.5)
        self._sock = sock
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        assert self._sock is not None
        while not self._stop.is_set():
            try:
                data, addr = self._sock.recvfrom(1024)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                self._handle(data, addr)
            except Exception:
                continue

    def _handle(self, data: bytes, addr) -> None:
        if len(data) < 48:
            return
        with self.lock:
            self.queries += 1
            cfg = Config(**asdict(self.cfg))
        try:
            req = ntp_proto.decode(data)
        except ntp_proto.NtpFormatError:
            return
        if req.mode != ntp_proto.MODE_CLIENT:
            return
        if cfg.drop_responses or not cfg.enabled:
            with self.lock:
                self.dropped += 1
            return

        client_tx_bits = ntp_proto.transmit_bits(data)
        now = time.time() + cfg.offset
        stratum = 16 if cfg.lie else cfg.stratum
        leap = 3 if cfg.lie else 0
        refid = b"\x00" * 4 if cfg.lie else cfg.refid.encode("ascii", "replace")[:4].ljust(4, b"\x00")
        origin_bits = client_tx_bits ^ (1 if cfg.spoof_origin else 0)
        # build reply manually so the origin field is copied bit-for-bit
        first = (leap << 6) | (4 << 3) | ntp_proto.MODE_SERVER
        header = struct.pack(
            "!BBbbII4s",
            first,
            stratum,
            3,
            -20,
            ntp_proto.short_float_bits(0.001),
            ntp_proto.short_float_bits(0.002),
            refid,
        )
        stamps = struct.pack(
            "!QQQQ",
            ntp_proto.to_ntp_bits(now - 10),
            origin_bits,
            ntp_proto.to_ntp_bits(now),
            ntp_proto.to_ntp_bits(now + 0.0001),
        )
        reply = header + stamps
        try:
            self._sock.sendto(reply, addr)  # type: ignore[union-attr]
        except OSError:
            return
        with self.lock:
            self.served += 1

    def stop(self) -> None:
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass

    # --------------------------------------------------------------- HTTP
    def snapshot(self) -> dict:
        with self.lock:
            now = time.time() + self.cfg.offset
            return {
                "name": self.name,
                "ntp_port": self.ntp_port,
                "http_port": self.http_port,
                "config": asdict(self.cfg),
                "now": now,
                "delta_from_host": self.cfg.offset,
                "queries": self.queries,
                "served": self.served,
                "dropped": self.dropped,
                "uptime": time.time() - self.started,
            }

    def update(self, payload: dict) -> dict:
        with self.lock:
            for key in ("offset", "enabled", "stratum", "lie", "spoof_origin", "drop_responses", "refid"):
                if key not in payload:
                    continue
                value = payload[key]
                if key == "offset":
                    value = float(value)
                elif key == "stratum":
                    value = int(value)
                    if not 0 <= value <= 16:
                        raise ValueError("stratum must be 0..16")
                elif key == "refid":
                    value = str(value)[:4]
                else:
                    value = bool(value)
                setattr(self.cfg, key, value)
        return self.snapshot()

    def step(self, delta: float) -> dict:
        with self.lock:
            self.cfg.offset += float(delta)
        return self.snapshot()


fake: FakeUpstream | None = None

app = FastAPI(title="Local NTP upstream")


def _page() -> str:
    assert fake is not None
    snap = fake.snapshot()
    cfg = snap["config"]
    return f"""<!doctype html><html lang="zh"><meta charset="utf-8">
<title>本地上游 {snap['name']}</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:720px;margin:2rem auto;padding:0 1rem;background:#111;color:#eee}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #333;padding:.35rem;text-align:left;vertical-align:top}}
code{{background:#222;padding:.1rem .3rem;border-radius:4px}}button{{padding:.4rem .8rem;margin:.15rem;background:#2d6cdf;color:#fff;border:0;border-radius:6px;cursor:pointer}}
input{{width:6rem;background:#222;color:#eee;border:1px solid #444;border-radius:4px;padding:.25rem}}
pre{{background:#1a1a1a;padding:.6rem;border-radius:6px;overflow:auto;max-height:16rem}}
</style>
<h1>本地上游 <code>{snap['name']}</code></h1>
<table>
<tr><th>NTP 端口</th><td>{snap['ntp_port']}/udp</td></tr>
<tr><th>当前伪时间</th><td id="now">{snap['now']:.3f}</td></tr>
<tr><th>相对宿主偏移</th><td>{cfg['offset']:+.3f} s</td></tr>
<tr><th>启用</th><td>{cfg['enabled']}</td></tr>
<tr><th>层数</th><td>{cfg['stratum']}</td></tr>
<tr><th>谎报未同步 (stratum 16/LI 3)</th><td>{cfg['lie']}</td></tr>
<tr><th>篡改 origin</th><td>{cfg['spoof_origin']}</td></tr>
<tr><th>丢弃应答</th><td>{cfg['drop_responses']}</td></tr>
<tr><th>查询/应答/丢弃</th><td>{snap['queries']} / {snap['served']} / {snap['dropped']}</td></tr>
</table>
<h3>调偏</h3>
<form method="post" action="/api/config">
  偏移 <input name="offset" value="{cfg['offset']}"> 秒
  <input type="hidden" name="enabled" value="1">
  <button>设置</button>
</form>
<h3>快速操作</h3>
<button onclick="post('/api/config',{{enabled:true,drop_responses:false}})">启用</button>
<button onclick="post('/api/config',{{enabled:false}})">停用</button>
<button onclick="post('/api/config',{{drop_responses:true}})">丢包</button>
<button onclick="post('/api/config',{{drop_responses:false}})">恢复应答</button>
<button onclick="post('/api/step',{{delta:-3600}})">回拨 1 小时</button>
<button onclick="post('/api/config',{{offset:0}})">对齐宿主</button>
<button onclick="post('/api/config',{{lie:true}})">谎报未同步</button>
<button onclick="post('/api/config',{{lie:false}})">停止谎报</button>
<button onclick="post('/api/config',{{spoof_origin:true}})">篡改 origin</button>
<button onclick="post('/api/config',{{spoof_origin:false}})">停止篡改</button>
<h3>状态 JSON</h3><pre id="json">{json.dumps(snap, indent=2, ensure_ascii=False)}</pre>
<script>
async function post(url, body) {{
  const r = await fetch(url, {{method:'POST', headers:{{'Content-Type':'application/json'}}, body: JSON.stringify(body)}});
  const j = await r.json(); document.getElementById('json').textContent = JSON.stringify(j,null,2);
  setTimeout(()=>location.reload(), 300);
}}
setInterval(async () => {{
  const r = await fetch('/api/state'); const j = await r.json();
  document.getElementById('now').textContent = j.now.toFixed(3);
}}, 1000);
</script>
</html>"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(_page())


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse(fake.snapshot() if fake else {})


@app.post("/api/config")
async def api_config(request: Request) -> JSONResponse:
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            payload = await request.json()
        else:
            form = await request.form()
            payload = dict(form)
            for key in ("enabled",):
                if key in payload:
                    payload[key] = payload[key] not in ("0", "false", " False")
        snap = fake.update(payload) if fake else {}
        return JSONResponse(snap)
    except (ValueError, TypeError) as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)


@app.post("/api/step")
async def api_step(request: Request) -> JSONResponse:
    payload = await request.json()
    return JSONResponse(fake.step(float(payload.get("delta", 0))))


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "enabled": fake.cfg.enabled if fake else False}


def main() -> None:
    global fake
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=os.environ.get("UPSTREAM_NAME", "upstream"))
    parser.add_argument("--ntp-port", type=int, default=int(os.environ.get("NTP_PORT", "123")))
    parser.add_argument("--http-port", type=int, default=int(os.environ.get("HTTP_PORT", "8081")))
    parser.add_argument("--offset", type=float, default=float(os.environ.get("INITIAL_OFFSET", "0")))
    parser.add_argument("--drop", action="store_true", default=os.environ.get("DROP_RESPONSES") == "1")
    args = parser.parse_args()

    fake = FakeUpstream(args.name, args.ntp_port, args.http_port)
    fake.cfg.offset = args.offset
    fake.cfg.drop_responses = args.drop
    fake.start()

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=args.http_port, log_level="warning")


if __name__ == "__main__":
    main()
