"""storage-simulator: a small file store on /data with a SIMULATED quota and
SIMULATED throttling (HTTP 503 ServerBusy). Not Azure; never reports Azure metrics."""
import asyncio
import logging
import os
import random
import re
import socket
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field
from starlette.routing import Match

from .logging_setup import setup_logging

setup_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("storage-simulator")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
FILES_DIR = DATA_DIR / "files"
QUOTA_BYTES = int(os.environ.get("QUOTA_BYTES", str(200 * 1024 * 1024)))
MAX_WRITE_BYTES = int(os.environ.get("MAX_WRITE_BYTES", str(10 * 1024 * 1024)))
CHECK_INTERVAL_S = float(os.environ.get("CHECK_INTERVAL_S", "15"))
IO_TIMEOUT_S = float(os.environ.get("IO_TIMEOUT_S", "5"))
HOSTNAME = socket.gethostname()
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
OPERATIONS = ("write", "read", "delete")
SUCCESS_CODE = {"write": "201", "read": "200", "delete": "204"}

# ---- metrics (one process, one replica -> plain counters are exact) ----
UP = Gauge("storage_simulator_up", "1 if the last write+read+delete probe of the data directory passed")
TRANSACTIONS = Counter("storage_simulator_transactions_total",
                       "Storage operations by outcome, including throttled (503) and rejected ones",
                       ["operation", "status_code"])
REQUESTS = Counter("storage_simulator_requests_total", "HTTP requests", ["method", "route", "status_code"])
THROTTLED = Counter("storage_simulator_throttled_requests_total",
                    "Requests rejected with SIMULATED 503 ServerBusy (not an Azure metric)", ["operation"])
USED = Gauge("storage_simulator_used_bytes", "Bytes used under the simulated quota (measured on disk)")
CAPACITY = Gauge("storage_simulator_capacity_bytes", "SIMULATED quota (app-level, not the real volume size)")
FILES = Gauge("storage_simulator_files", "Number of stored files (measured on disk)")
OP_SECONDS = Histogram("storage_simulator_operation_duration_seconds", "Duration of storage operations that reached the disk",
                       ["operation"], buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
THROTTLE_ENABLED = Gauge("storage_simulator_throttling_enabled", "1 while simulated throttling is enabled")
THROTTLE_RATE = Gauge("storage_simulator_throttling_rate", "Share of storage requests rejected while throttling is enabled (0-1)")
LAST_CHECK = Gauge("storage_simulator_last_check_timestamp_seconds", "Unix time the last background probe finished")

CAPACITY.set(QUOTA_BYTES)
UP.set(0)  # becomes 1 only after a real probe passes
THROTTLE_ENABLED.set(0)
THROTTLE_RATE.set(0)
for op in OPERATIONS:  # pre-create so rate() sees the first events
    THROTTLED.labels(op)
    OP_SECONDS.labels(op)
    TRANSACTIONS.labels(op, SUCCESS_CODE[op])
    TRANSACTIONS.labels(op, "503")

throttle = {"enabled": False, "rate": 0.0}
used_bytes = 0          # cached; corrected by the background scan
state = {"up": False, "files": 0, "last_check": None}
write_lock = asyncio.Lock()           # serializes writes/deletes -> quota check is race-free
probe_lock = threading.Lock()         # held by the probe THREAD; stays held if the disk hangs


def _scan() -> tuple[int, int]:
    total = count = 0
    with os.scandir(FILES_DIR) as it:
        for entry in it:
            if entry.is_file(follow_symlinks=False) and not entry.name.startswith("."):
                total += entry.stat(follow_symlinks=False).st_size
                count += 1
    return total, count


def _probe_and_scan() -> tuple[int, int]:
    if not probe_lock.acquire(blocking=False):
        raise RuntimeError("previous probe still running")
    try:
        FILES_DIR.mkdir(parents=True, exist_ok=True)
        probe = DATA_DIR / f".probe-{HOSTNAME}"
        token = uuid.uuid4().hex
        probe.write_text(token)
        if probe.read_text() != token:
            raise IOError("read-back mismatch")
        probe.unlink()
        return _scan()
    finally:
        probe_lock.release()


async def check_once() -> None:
    global used_bytes
    try:
        async with write_lock:   # no write can happen between scan and assignment
            total, count = await asyncio.wait_for(asyncio.to_thread(_probe_and_scan), IO_TIMEOUT_S)
            used_bytes = total
        USED.set(total)
        FILES.set(count)
        UP.set(1)
        state.update(up=True, files=count)
    except Exception as exc:
        UP.set(0)
        state["up"] = False
        log.error("storage probe failed", extra={"fields": {"error": f"{exc.__class__.__name__}: {exc}"}})
    state["last_check"] = time.time()
    LAST_CHECK.set(state["last_check"])


async def check_loop() -> None:
    while True:
        await check_once()
        await asyncio.sleep(CHECK_INTERVAL_S)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(check_loop())
    log.info("startup complete", extra={"fields": {"data_dir": str(DATA_DIR), "quota_bytes": QUOTA_BYTES}})
    yield
    task.cancel()


app = FastAPI(title="storage-simulator", lifespan=lifespan)


def route_template(request: Request) -> str:
    for route in request.app.router.routes:
        match, _ = route.matches(request.scope)
        if match == Match.FULL:
            return getattr(route, "path", "unmatched")
    return "unmatched"


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    route = route_template(request)
    REQUESTS.labels(request.method, route, str(response.status_code)).inc()
    if route not in ("/healthz", "/metrics"):
        log.info("request", extra={"fields": {
            "method": request.method, "route": route, "path": request.url.path,
            "status": response.status_code, "duration_ms": round((time.perf_counter() - start) * 1000, 1)}})
    return response


def result(op: str, status: int, body=None, headers=None, raw: bytes | None = None) -> Response:
    TRANSACTIONS.labels(op, str(status)).inc()
    if raw is not None:
        return Response(raw, status_code=status, media_type="application/octet-stream", headers=headers)
    if status == 204:
        return Response(status_code=204, headers=headers)
    return JSONResponse(body, status_code=status, headers=headers)


def throttled(op: str) -> Response | None:
    if throttle["enabled"] and random.random() < throttle["rate"]:
        THROTTLED.labels(op).inc()
        return result(op, 503, {"error": {"code": "ServerBusy",
                                          "message": "Simulated throttling (storage-simulator, not Azure)."}},
                      headers={"Retry-After": "1"})
    return None


def check_name(op: str, filename: str) -> Response | None:
    if not NAME_RE.match(filename):
        return result(op, 400, {"error": {"code": "InvalidName", "message": "filename must match " + NAME_RE.pattern}})
    return None


async def io(fn, *args):
    """Run blocking file I/O in a thread with a timeout: a hung NFS mount never blocks the event loop."""
    return await asyncio.wait_for(asyncio.to_thread(fn, *args), IO_TIMEOUT_S)


async def disk(op: str, fn, *args):
    start = time.perf_counter()
    try:
        return await io(fn, *args)
    finally:
        OP_SECONDS.labels(op).observe(time.perf_counter() - start)


def _size_or_none(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return None


def _write(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)   # atomic: readers never see a half-written file


def _delete(path: Path) -> int:
    size = path.stat().st_size
    path.unlink()
    return size


class WriteIn(BaseModel):
    filename: str
    size_bytes: int = Field(ge=1)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/storage/write")
async def storage_write(body: WriteIn):
    global used_bytes
    op = "write"
    if (r := throttled(op)) or (r := check_name(op, body.filename)):
        return r
    if body.size_bytes > MAX_WRITE_BYTES:
        return result(op, 413, {"error": {"code": "TooLarge", "message": f"max {MAX_WRITE_BYTES} bytes per write"}})
    path = FILES_DIR / body.filename
    async with write_lock:
        try:
            existing = await io(_size_or_none, path)
        except Exception as exc:
            return result(op, 500, {"error": {"code": "IOError", "message": str(exc) or exc.__class__.__name__}})
        if used_bytes - (existing or 0) + body.size_bytes > QUOTA_BYTES:
            return result(op, 507, {"error": {"code": "QuotaExceeded",
                                              "message": "Simulated quota exceeded",
                                              "used_bytes": used_bytes, "capacity_bytes": QUOTA_BYTES}})
        try:
            await disk(op, _write, path, os.urandom(body.size_bytes))
        except Exception as exc:
            log.error("write failed", extra={"fields": {"file": body.filename, "error": f"{exc.__class__.__name__}: {exc}"}})
            return result(op, 500, {"error": {"code": "IOError", "message": str(exc) or exc.__class__.__name__}})
        used_bytes += body.size_bytes - (existing or 0)
        USED.set(used_bytes)
        if existing is None:            # new file (overwrite keeps the count)
            state["files"] += 1
            FILES.set(state["files"])
    return result(op, 201, {"filename": body.filename, "size_bytes": body.size_bytes, "used_bytes": used_bytes})


@app.get("/storage/read/{filename}")
async def storage_read(filename: str):
    op = "read"
    if (r := throttled(op)) or (r := check_name(op, filename)):
        return r
    try:
        data = await disk(op, (FILES_DIR / filename).read_bytes)
    except FileNotFoundError:
        return result(op, 404, {"error": {"code": "NotFound", "message": filename}})
    except Exception as exc:
        log.error("read failed", extra={"fields": {"file": filename, "error": f"{exc.__class__.__name__}: {exc}"}})
        return result(op, 500, {"error": {"code": "IOError", "message": str(exc) or exc.__class__.__name__}})
    return result(op, 200, raw=data)


@app.delete("/storage/delete/{filename}")
async def storage_delete(filename: str):
    global used_bytes
    op = "delete"
    if (r := throttled(op)) or (r := check_name(op, filename)):
        return r
    async with write_lock:
        try:
            size = await disk(op, _delete, FILES_DIR / filename)
        except FileNotFoundError:
            return result(op, 404, {"error": {"code": "NotFound", "message": filename}})
        except Exception as exc:
            return result(op, 500, {"error": {"code": "IOError", "message": str(exc) or exc.__class__.__name__}})
        used_bytes -= size
        USED.set(used_bytes)
        state["files"] -= 1
        FILES.set(state["files"])
    return result(op, 204)


@app.get("/storage/status")
def storage_status():
    return {
        "simulated": True,
        "note": "App-level quota and throttling are SIMULATED; not Azure metrics.",
        "up": state["up"],
        "used_bytes": used_bytes,
        "capacity_bytes": QUOTA_BYTES,
        "used_percent": round(100 * used_bytes / QUOTA_BYTES, 2),
        "files": state["files"],
        "throttling": dict(throttle),
        "last_check_age_s": round(time.time() - state["last_check"], 1) if state["last_check"] else None,
    }


class ThrottleIn(BaseModel):
    rate: float = Field(default=1.0, gt=0, le=1)


@app.post("/simulation/throttling/enable")
def throttling_enable(body: ThrottleIn | None = None):
    rate = (body or ThrottleIn()).rate
    throttle.update(enabled=True, rate=rate)
    THROTTLE_ENABLED.set(1)
    THROTTLE_RATE.set(rate)
    log.warning("simulated throttling ENABLED", extra={"fields": {"rate": rate}})
    return {"throttling": dict(throttle)}


@app.post("/simulation/throttling/disable")
def throttling_disable():
    throttle.update(enabled=False, rate=0.0)
    THROTTLE_ENABLED.set(0)
    THROTTLE_RATE.set(0)
    log.warning("simulated throttling DISABLED")
    return {"throttling": dict(throttle)}
