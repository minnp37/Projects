"""
OCPP 1.6J virtual charger (mimics a Wallbox Pulsar Plus) for Octopus Energy.

Runs as a Home Assistant add-on (FastAPI UI on ingress + MQTT entities).

What it does
  * Connects to the Octopus CMS and behaves like the real charger observed in the
    26 Oct 2026 capture (message shapes, timings, status sequence).
  * Power and the lifetime energy register are *inputs*: set them from the UI,
    MQTT or an HA automation. Energy is integrated from power unless
    energy_source = "mqtt".
  * Logs every raw OCPP frame in both directions to DATA_DIR/captures/*.jsonl
    and lists the commands Octopus sends in the UI.

Needs: websockets==10.4 (legacy API), ocpp, fastapi, uvicorn, paho-mqtt (1.x or 2.x).
"""
import asyncio
import base64
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
import websockets
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ocpp.routing import on
from ocpp.v16 import ChargePoint, call, call_result
from ocpp.v16.enums import Action

# ============================================================================
# Paths and options
# ============================================================================
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
APP_DIR = Path(__file__).resolve().parent
STATE_FILE = DATA_DIR / "session_state.json"
CONFIG_STORE = DATA_DIR / "charger_config.json"            # runtime copy (ChangeConfiguration)
DEFAULT_CONFIG_STORE = APP_DIR / "charger_config.json"      # shipped copy of the real charger's keys
CAPTURE_DIR = DATA_DIR / "captures"


def _load_options() -> dict:
    path = DATA_DIR / "options.json"
    if not path.exists():
        raise SystemExit(f"No options file at {path} (set DATA_DIR or create options.json)")
    return json.loads(path.read_text())


OPTIONS = _load_options()


def opt(key, default=None, section=None):
    """Read an option; a missing key or a null value both give `default`."""
    block = OPTIONS.get(section) or {} if section else OPTIONS
    value = block.get(key)
    return default if value is None else value


CHARGEPOINT_ID = opt("chargepoint_id", section="charger")
PASSWORD = opt("password", section="charger")
USER_AGENT = opt("user_agent", "WebSocket++/0.8.2", "charger")  # what the real Wallbox sends
DEFAULT_ID_TAG = opt("default_id_tag", "ffffffffffffff7f", "charger")
LOG_LINES = int(opt("log_lines", 500))
WS_PING_INTERVAL = float(opt("ws_ping_interval", 50))          # real charger: WebSocketPingInterval=50
ENERGY_SOURCE = opt("energy_source", "integrate")               # "integrate" | "mqtt"
VOLTAGE_V = float(opt("voltage_v", 226))                        # only used for Power.Offered
MANUAL_STOP_LOCKOUT_S = float(opt("manual_stop_lockout_s", 60))
LOG_RAW_FRAMES = bool(opt("log_raw_frames", True))
QUIRK_ALIGNED_LAG = bool(opt("aligned_timestamp_lag", True, "quirks"))      # clock samples stamped 1 interval late
QUIRK_BOOT_ZERO = bool(opt("boot_sample_energy_zero", False, "quirks"))     # real charger sends 0.0 Wh at boot
BOOT_ON_RECONNECT = bool(opt("boot_on_reconnect", False, "quirks"))         # real charger does not re-boot
T_PREPARE = float(opt("prepare_s", 1.5, "timing"))                # RemoteStart -> Preparing
T_SUSPENDED = float(opt("suspended_ev_s", 0.5, "timing"))         # Preparing -> SuspendedEV
T_START_TX = float(opt("start_transaction_s", 0.15, "timing"))    # SuspendedEV -> StartTransaction
T_CHARGING = float(opt("charging_s", 6.0, "timing"))              # StartTransaction.conf -> Charging
CALL_TIMEOUT = 15
RECONNECT_DELAY = 10
OUTBOX_MAX = 1000
CAPTURE_FILE_BYTES = 5 * 1024 * 1024     # start a new capture file after 5 MB
CAPTURE_TOTAL_BYTES = 90 * 1024 * 1024   # delete the oldest files beyond 90 MB in total (stays under 100 MB)
CAPTURE_MAX_FILES = 50


def build_url() -> str:
    """server_url is used exactly as given; the chargepoint id is appended unless it is already the last part."""
    base = str(opt("server_url", "", "charger")).rstrip("/")
    if not base.endswith(str(CHARGEPOINT_ID)):
        base += f"/{CHARGEPOINT_ID}"
    return base


SERVER_URL = build_url()

# ============================================================================
# Logging (UI ring buffer + raw frame capture)
# ============================================================================
logger = logging.getLogger("virtual_charger")
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%dT%H:%M:%S"))
logger.handlers[:] = [_handler]
logging.getLogger("ocpp").setLevel(logging.WARNING)   # the library logs every frame at INFO

log_buffer: deque = deque(maxlen=LOG_LINES)


def utc_iso(epoch: float | None = None) -> str:
    """Second-precision UTC timestamp, as the real charger sends."""
    return datetime.fromtimestamp(epoch if epoch is not None else time.time(), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str):
    log_buffer.append(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')} {msg}")
    logger.info(msg)


class FrameLog:
    """JSONL capture of every raw OCPP frame plus the commands Octopus sent."""

    def __init__(self, enabled: bool):
        self.enabled = enabled
        self.commands: deque = deque(maxlen=50)   # inbound CALLs, newest last
        self._f = None
        self._size = 0
        if enabled:
            CAPTURE_DIR.mkdir(parents=True, exist_ok=True)
            self._open_new()

    def _open_new(self):
        """Open a fresh capture file, then delete the oldest ones if the folder is over its limits."""
        if self._f:
            self._f.close()
        base = f"octopus_frames_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        path, n = CAPTURE_DIR / f"{base}.jsonl", 1
        while path.exists():                      # never reuse a name (two files in the same second)
            path, n = CAPTURE_DIR / f"{base}_{n}.jsonl", n + 1
        self._f = open(path, "a", encoding="utf-8", buffering=1)
        self._size = 0
        files = sorted(CAPTURE_DIR.glob("octopus_frames_*.jsonl"), key=lambda f: f.stat().st_mtime_ns)   # oldest first
        total = sum(f.stat().st_size for f in files)
        while len(files) > 1 and (total > CAPTURE_TOTAL_BYTES or len(files) > CAPTURE_MAX_FILES):
            total -= files[0].stat().st_size
            files.pop(0).unlink(missing_ok=True)   # oldest first, never the file just opened

    def _write(self, rec: dict):
        if self._f:
            line = json.dumps(rec, ensure_ascii=False) + "\n"
            self._f.write(line)
            self._f.flush()
            self._size += len(line)
            if self._size > CAPTURE_FILE_BYTES:
                self._open_new()

    def frame(self, direction: str, raw):
        raw = raw if isinstance(raw, str) else raw.hex()
        rec = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
               "dir": direction, "raw": raw}
        try:
            data = json.loads(raw)
            if direction == "IN" and data[0] == 2:     # a command from Octopus
                self.commands.append({"ts": rec["ts"], "action": data[2], "payload": data[3] if len(data) > 3 else {}})
                log(f"⬇️ OCTOPUS COMMAND {data[2]} {json.dumps(data[3] if len(data) > 3 else {})[:300]}")
            elif data[0] == 4:
                log(f"⚠️ CALLERROR {raw[:300]}")
        except Exception:
            pass
        self._write(rec)

    def event(self, name: str, **extra):
        self._write({"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                     "dir": "EVENT", "event": name, **extra})


frames = FrameLog(LOG_RAW_FRAMES)


class LoggingConnection:
    """Wraps the websocket so every raw frame is captured, whatever the ocpp library does."""

    def __init__(self, ws):
        self._ws = ws

    async def recv(self):
        msg = await self._ws.recv()
        frames.frame("IN", msg)
        return msg

    async def send(self, msg):
        await self._ws.send(msg)
        frames.frame("OUT", msg)

    def __getattr__(self, name):
        return getattr(self._ws, name)


# ============================================================================
# MQTT (Home Assistant entities)
# ============================================================================
DEVICE_ID = "virtual_charger"
BASE_TOPIC = f"ha/charger/{DEVICE_ID}"
DISCOVERY_PREFIX = "homeassistant"


class Mqtt:
    def __init__(self):
        self.enabled = os.getenv("MQTT_DISABLED") != "1"
        self.client = None
        self.connected = False

    def start(self, on_command):
        """on_command(topic_suffix, payload) is called from the MQTT thread."""
        if not self.enabled:
            log("📡 MQTT disabled (MQTT_DISABLED=1)")
            return
        host = os.getenv("MQTT_HOST") or opt("host", "localhost", "mqtt")
        port = int(os.getenv("MQTT_PORT") or opt("port", 1883, "mqtt"))
        try:
            try:
                self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)   # paho-mqtt 2.x
            except AttributeError:
                self.client = mqtt.Client()                                    # paho-mqtt 1.x
            if opt("username", section="mqtt"):
                self.client.username_pw_set(opt("username", section="mqtt"), opt("password", section="mqtt"))
            if opt("ssl", False, "mqtt"):
                self.client.tls_set()
            self.client.will_set(f"{BASE_TOPIC}/availability", "offline", retain=True)

            def _on_connect(client, userdata, flags, rc):
                self.connected = rc == 0
                log(f"📡 MQTT connected (rc={rc})")
                client.publish(f"{BASE_TOPIC}/availability", "online", retain=True)
                if os.getenv("SUPERVISOR_TOKEN") or os.getenv("MQTT_DISCOVERY") == "1":
                    self._discovery()
                client.subscribe(f"{BASE_TOPIC}/#")
                on_command("__connected__", "")

            def _on_message(client, userdata, msg):
                if msg.topic.startswith(BASE_TOPIC + "/") and msg.topic.endswith("/set"):
                    on_command(msg.topic[len(BASE_TOPIC) + 1:], msg.payload.decode())

            self.client.on_connect = _on_connect
            self.client.on_message = _on_message
            self.client.on_disconnect = lambda *a: setattr(self, "connected", False)
            self.client.connect_async(host, port, keepalive=60)   # never blocks or raises at startup
            self.client.loop_start()
            log(f"📡 MQTT connecting to {host}:{port}")
        except Exception as e:
            log(f"❌ MQTT init failed: {type(e).__name__}: {e}")
            self.client = None

    def stop(self):
        if self.client:
            try:
                self.client.publish(f"{BASE_TOPIC}/availability", "offline", retain=True)
                self.client.loop_stop()
            except Exception:
                pass

    def pub(self, suffix: str, payload, retain=True):
        if self.client and self.connected:
            self.client.publish(f"{BASE_TOPIC}/{suffix}", str(payload), retain=retain)

    def _discovery(self):
        device = {"identifiers": [DEVICE_ID], "name": "OCPP Virtual Charger",
                  "model": "OCPP Simulator V1", "manufacturer": "Custom Python"}
        avail = {"availability_topic": f"{BASE_TOPIC}/availability"}

        def entity(kind, uid, cfg):
            self.client.publish(f"{DISCOVERY_PREFIX}/{kind}/{DEVICE_ID}_{uid}/config",
                                json.dumps({**cfg, "unique_id": f"{DEVICE_ID}_{uid}", "device": device, **avail}),
                                retain=True)
        # unique ids of the first four are unchanged from the previous version
        entity("button", "stop", {"name": "Stop Charging", "command_topic": f"{BASE_TOPIC}/switch/set", "icon": "mdi:ev-plug-type2"})
        entity("button", "prep", {"name": "Set Preparing State", "command_topic": f"{BASE_TOPIC}/button/preparing/set", "icon": "mdi:ev-plug-type2"})
        entity("number", "power", {"name": "Charging Power", "command_topic": f"{BASE_TOPIC}/number/power/set",
                                   "state_topic": f"{BASE_TOPIC}/number/power/state", "min": 0, "max": 7400, "step": 1,
                                   "mode": "box", "unit_of_measurement": "W"})
        entity("sensor", "meter", {"name": "Energy Meter", "state_topic": f"{BASE_TOPIC}/sensor/meter",
                                   "unit_of_measurement": "kWh", "device_class": "energy", "state_class": "total_increasing"})
        # new
        entity("button", "start", {"name": "Start Charging", "command_topic": f"{BASE_TOPIC}/button/start/set", "icon": "mdi:ev-station"})
        entity("number", "meter_set", {"name": "Lifetime Meter Set", "command_topic": f"{BASE_TOPIC}/number/meter/set",
                                       "state_topic": f"{BASE_TOPIC}/sensor/meter", "min": 0, "max": 100000000, "step": 0.001,
                                       "mode": "box", "unit_of_measurement": "kWh"})
        entity("sensor", "status", {"name": "Connector Status", "state_topic": f"{BASE_TOPIC}/sensor/status"})
        entity("select", "status_set", {"name": "Set Connector Status", "command_topic": f"{BASE_TOPIC}/select/status/set",
                                        "options": ["Available", "Preparing", "SuspendedEV", "Finishing"]})


mqtt_bridge = Mqtt()

# ============================================================================
# Measurands: what goes into MeterValues (driven by the config keys
# MeterValuesSampledData / MeterValuesAlignedData, as on the real charger)
# ============================================================================
# measurand -> (unit or None, value function).  Frequency has no unit on the real charger.
# SoC is in the real charger's config but never sent (the car doesn't provide it).
SAMPLERS = {
    "Energy.Active.Import.Register": ("Wh", lambda c: f"{c.meter_wh:.1f}"),
    "Power.Active.Import": ("W", lambda c: f"{c.reported_power():.1f}"),
    "Frequency": (None, lambda c: "50.0"),
    "Power.Offered": ("W", lambda c: str(int(c.current_offered * VOLTAGE_V))),
    "Current.Offered": ("A", lambda c: str(int(c.current_offered))),
    "Energy.Active.Export.Register": ("Wh", lambda c: "0"),
    "Power.Active.Export": ("W", lambda c: "0"),
}
class Snapshot:
    """Charger values at some other moment (used for samples the charger 'took' while the process was frozen)."""

    def __init__(self, base, meter_wh: float, power_w: float):
        self.meter_wh, self._power, self.current_offered = meter_wh, power_w, base.current_offered

    def reported_power(self) -> float:
        return self._power


STATUSES = {"available": "Available", "preparing": "Preparing", "suspendedev": "SuspendedEV",
            "suspendedevse": "SuspendedEVSE", "finishing": "Finishing", "faulted": "Faulted"}


# ============================================================================
# The charger itself (long-lived state, independent of the websocket)
# ============================================================================
class Charger:
    def __init__(self):
        self.session = None            # current Session (websocket) or None
        self.ready = False             # BootNotification accepted on this connection
        self.booted = False            # has booted at least once since process start
        self.status = "Available"
        self.tx_id = None
        self.tx_id_tag = None
        self.power_w = float(opt("dummy_power_watts", 0))
        self.meter_wh = float(opt("initial_meter_wh", 0, "charger"))
        self.outbox: deque = deque()   # transaction messages queued while offline
        self.config: dict = {}
        self.profiles: list = []       # charging profiles Octopus sent (logged, not acted on yet)
        self.local_list_version = 0
        self.lock = asyncio.Lock()
        self.tasks: set = set()
        self.periodic_task = None
        self.flush_task = None
        self.manual_stop_at = None
        self._last_save = 0.0
        self.last_activity = time.monotonic()   # heartbeat timer
        self.last_periodic_wall = None          # epoch time of the last 60 s sample (for back-filling gaps)
        self.tx_start_meter = 0.0
        self.last_boundary = None               # last clock-aligned boundary reported
        self._energy_last = time.monotonic()
        self._dirty = False
        self._warned: set = set()
        self._override_applied = None           # last meter_wh_override value that was applied (kept in the state file)
        self._load_config_store()
        self._load_state()
        self._apply_meter_override()

    # ---------- helpers ----------
    def spawn(self, coro):
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)
        t.add_done_callback(lambda t: t.cancelled() or t.exception() and log(f"❌ Task error: {t.exception()!r}"))
        return t

    def cfg(self, key, default=""):
        return self.config.get(key, {}).get("value", default)

    @property
    def meter_interval(self) -> int:
        return int(self.cfg("MeterValueSampleInterval", 60) or 60)

    @property
    def aligned_interval(self) -> int:
        return int(self.cfg("ClockAlignedDataInterval", 900) or 0)

    @property
    def heartbeat_interval(self) -> int:
        return int(self.cfg("HeartbeatInterval", 300) or 300)

    @property
    def current_offered(self) -> float:
        return float(self.cfg("chargingALimitConn1", 32) or 32)

    @property
    def offered_w(self) -> float:
        return self.current_offered * VOLTAGE_V

    def reported_power(self) -> float:
        """What the car draws: the power setpoint, capped at Power.Offered (a charger cannot exceed it)."""
        if self.status == "Charging" and self.tx_id is not None:
            return min(self.power_w, self.offered_w)
        return 0.0

    # ---------- persistence ----------
    def _load_config_store(self):
        src = CONFIG_STORE if CONFIG_STORE.exists() else DEFAULT_CONFIG_STORE
        items = json.loads(src.read_text())
        self.config = {i["key"]: {"value": str(i["value"]), "readonly": bool(i["readonly"])} for i in items}
        # identity values come from options, not from the captured real charger
        serial = opt("chargepoint_serial_number", section="charger")
        for key, value in (("SerialNumber", serial), ("WebSocketUrl", SERVER_URL.rsplit("/", 1)[0]),
                           ("WebSocketUser", CHARGEPOINT_ID), ("WebSocketPassword", ""),
                           ("HeartbeatInterval", opt("heartbeat_interval")),
                           ("MeterValueSampleInterval", opt("meter_interval")),
                           ("ClockAlignedDataInterval", opt("clock_aligned_interval"))):
            if value is not None and key in self.config:
                self.config[key]["value"] = str(value)

    def _save_config_store(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_STORE.write_text(json.dumps([{"key": k, "value": v["value"], "readonly": v["readonly"]}
                                            for k, v in self.config.items()], indent=1))

    def _load_state(self):
        if not STATE_FILE.exists():
            log(f"ℹ️ No saved state; meter seeded at {self.meter_wh:.0f} Wh (initial_meter_wh)")
            return
        try:
            d = json.loads(STATE_FILE.read_text())
            self.meter_wh = float(d.get("meter_wh", self.meter_wh))
            self._override_applied = d.get("override_applied")
            self.power_w = float(d.get("power_w", d.get("dummy_power_watts", self.power_w)))   # old key still read
            self.tx_id = d.get("transaction_id", d.get("current_transaction_id"))
            self.tx_id_tag = d.get("id_tag")
            self.status = d.get("status") or ("Charging" if d.get("is_charging") else "Available")
            if self.tx_id is None and self.status in ("Charging", "SuspendedEV"):
                self.status = "Preparing"
            log(f"📂 State loaded: status={self.status} tx={self.tx_id} meter={self.meter_wh:.1f} Wh power={self.power_w:.0f} W")
        except Exception as e:
            log(f"⚠️ Could not load state: {e}")

    def _apply_meter_override(self):
        """One-time reset of the lifetime meter. If meter_wh_override is set and differs from the value applied last
        time, the meter takes that value (beating any saved state) and the value is remembered, so later restarts
        carry on from the saved state. Change the number to apply it again."""
        override = opt("meter_wh_override", section="charger")
        if override is None or float(override) == self._override_applied:
            return
        log(f"🔢 meter_wh_override: lifetime meter set to {float(override):.0f} Wh (was {self.meter_wh:.1f} Wh)")
        self.meter_wh, self._override_applied = float(override), float(override)
        if self.tx_id is not None:     # a transaction left in an old state file is not one Octopus knows about
            log("ℹ️ Ignoring the transaction in the old saved state")
            self.tx_id, self.tx_id_tag, self.status = None, None, "Available"
        self.save_state(force=True)

    def save_state(self, force=False):
        """Atomic write; throttled to every 30 s unless forced (energy changes every second)."""
        now = time.monotonic()
        if not force and now - self._last_save < 30:
            self._dirty = True
            return
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps({"status": self.status, "transaction_id": self.tx_id, "id_tag": self.tx_id_tag,
                                       "meter_wh": round(self.meter_wh, 3), "power_w": self.power_w,
                                       "override_applied": self._override_applied}))
            tmp.replace(STATE_FILE)
            self._last_save, self._dirty = now, False
        except Exception as e:
            log(f"⚠️ Could not save state: {e}")

    def publish_state(self):
        mqtt_bridge.pub("number/power/state", f"{self.power_w:g}")
        mqtt_bridge.pub("sensor/meter", round(self.meter_wh / 1000, 3))
        mqtt_bridge.pub("sensor/status", self.status)
        mqtt_bridge.pub("switch/state", "ON" if self.status == "Charging" else "OFF")

    # ---------- inputs from UI / MQTT / automations ----------
    def set_power(self, watts: float):
        self.power_w = max(0.0, watts)
        self.save_state(force=True)
        self.publish_state()
        log(f"⚡ Power set to {self.power_w:g} W")
        if self.power_w > self.offered_w:
            log(f"⚠️ Above Power.Offered ({self.offered_w:.0f} W) - reporting {self.offered_w:.0f} W")

    def set_meter_wh(self, wh: float):
        if self.meter_wh > 0 and (wh > self.meter_wh * 10 or wh < self.meter_wh / 10):
            log(f"⚠️ Meter changes by more than 10x ({self.meter_wh:.0f} -> {wh:.0f} Wh) - check you entered Wh, not kWh")
        if wh < self.meter_wh:
            log(f"⚠️ Meter lowered {self.meter_wh:.1f} -> {wh:.1f} Wh (Octopus will see the register go backwards)")
        self.meter_wh = max(0.0, wh)
        self.save_state(force=True)
        self.publish_state()
        log(f"🔢 Lifetime meter set to {self.meter_wh:.1f} Wh")

    async def command(self, cmd: str):
        c = (cmd or "").strip().lower()
        if c in ("charge", "start"):
            if self.tx_id is not None or self.lock.locked():
                log("ℹ️ Already charging / start in progress")
                return
            self.spawn(self.start_sequence(DEFAULT_ID_TAG, 1))
        elif c == "stop":
            self.manual_stop_at = time.monotonic()
            self.spawn(self.stop_sequence())
        elif c in STATUSES:
            if self.tx_id is not None:
                log("ℹ️ Transaction active - stop it before setting a status by hand")
                return
            await self.set_status(STATUSES[c])
        else:
            log(f"❓ Unknown command: {cmd}")

    def mqtt_command(self, topic: str, payload: str):
        """Called from the MQTT thread."""
        if topic == "__connected__":
            self.publish_state()
            return
        log(f"📩 MQTT {topic} = {payload}")

        async def run():
            try:
                if topic == "switch/set":
                    await self.command("stop")
                elif topic == "button/preparing/set":
                    await self.command("preparing")
                elif topic == "button/start/set":
                    await self.command("charge")
                elif topic == "select/status/set":
                    await self.command(payload)
                elif topic == "number/power/set":
                    self.set_power(float(payload))
                elif topic == "number/meter/set":
                    self.set_meter_wh(float(payload) * 1000)
            except ValueError:
                log(f"⚠️ Bad MQTT payload for {topic}: {payload!r}")
        asyncio.run_coroutine_threadsafe(run(), LOOP)

    # ---------- sending ----------
    async def send(self, req, queue=False):
        """Send if online. queue=True keeps transaction messages (original timestamp) for replay after a reconnect."""
        if queue and (not self.ready or self.outbox):
            if len(self.outbox) < OUTBOX_MAX:
                self.outbox.append(req)
            log(f"📥 Offline - queued {type(req).__name__} ({len(self.outbox)} waiting)")
            if self.ready:
                self._kick_flush()
            return None
        if not self.ready:
            return None
        try:
            return await self.session.call(req)
        except Exception as e:
            log(f"❌ {type(req).__name__} failed: {type(e).__name__}: {e}")
            if queue and len(self.outbox) < OUTBOX_MAX:
                self.outbox.append(req)
            return None

    def _kick_flush(self):
        if not self.flush_task or self.flush_task.done():
            self.flush_task = self.spawn(self._flush())

    async def _flush(self):
        while self.outbox and self.ready:
            try:
                await self.session.call(self.outbox[0])
                self.outbox.popleft()
            except Exception as e:
                log(f"❌ Replay failed, will retry on next connection: {type(e).__name__}: {e}")
                return
        log("📤 Offline queue replayed")

    async def send_status(self, connector: int, status: str):
        await self.send(call.StatusNotification(
            connector_id=connector, error_code="NoError", status=status, timestamp=utc_iso(),
            info="", vendor_id="com.wallbox", vendor_error_code=""))

    async def set_status(self, status: str):
        self.status = status
        self.save_state(force=True)
        self.publish_state()
        log(f"🔌 Connector status -> {status}")
        await self.send_status(1, status)

    # ---------- meter values ----------
    def sampled_values(self, csv: str, context: str, energy_override=None, snap=None) -> list:
        src = snap or self
        out = []
        for name in [m.strip() for m in csv.split(",") if m.strip()]:
            sampler = SAMPLERS.get(name)
            if not sampler:
                if name not in self._warned:
                    self._warned.add(name)
                    log(f"ℹ️ Measurand {name} is configured but not simulated - skipped")
                continue
            unit, fn = sampler
            item = {"format": "Raw", "location": "Outlet", "context": context, "measurand": name}
            if unit:
                item["unit"] = unit
            item["value"] = energy_override if (energy_override and name == "Energy.Active.Import.Register") else fn(src)
            out.append(item)
        return out

    async def send_periodic(self, connector=1, context="Sample.Periodic"):
        samples = self.sampled_values(self.cfg("MeterValuesSampledData"), context)
        req = call.MeterValues(connector_id=connector, transaction_id=self.tx_id,
                               meter_value=[{"timestamp": utc_iso(), "sampledValue": samples}])
        if context == "Sample.Periodic":
            self.last_periodic_wall = time.time()
        await self.send(req, queue=self.tx_id is not None)
        mqtt_bridge.pub("sensor/meter", round(self.meter_wh / 1000, 3))

    def estimate_at(self, t: float, now: float) -> tuple:
        """(register Wh, power W) at past time t, assuming the current state held since then. That is exactly how the
        energy integrator treated the gap, so back-filled samples stay consistent with the register."""
        if self.tx_id is not None and self.status == "Charging":
            p = self.reported_power()
            return max(self.tx_start_meter, self.meter_wh - p * (now - t) / 3600.0), p
        return self.meter_wh, 0.0

    async def backfill_periodic(self, now: float):
        """A real charger keeps sampling every 60 s while offline and sends the lot afterwards. If this process was
        frozen (PC sleep) or stalled, create the samples it would have taken, with their original timestamps."""
        interval = self.meter_interval
        t, n = self.last_periodic_wall + interval, 0
        while t <= now - interval * 0.5 and n < 600:
            e, p = self.estimate_at(t, now)
            samples = self.sampled_values(self.cfg("MeterValuesSampledData"), "Sample.Periodic", snap=Snapshot(self, e, p))
            await self.send(call.MeterValues(connector_id=1, transaction_id=self.tx_id,
                                             meter_value=[{"timestamp": utc_iso(t), "sampledValue": samples}]), queue=True)
            t, n = t + interval, n + 1
        if n:
            log(f"🕓 Back-filled {n} missed 60 s samples ({utc_iso(self.last_periodic_wall + interval)} onwards)")

    async def send_aligned_pair(self, stamp: float | None = None, boot=False, snap=None):
        """Connector 0 then connector 1, like the real charger. Clock-aligned samples carry the *previous*
        boundary's timestamp on the real charger (quirk), values are current."""
        interval = self.aligned_interval or 900
        if stamp is None:                                    # boot-time pair
            stamp = time.time() - (interval if QUIRK_ALIGNED_LAG else 0)
        override = "0.0" if (boot and QUIRK_BOOT_ZERO) else None
        samples = self.sampled_values(self.cfg("MeterValuesAlignedData"), "Sample.Clock", override, snap)   # one snapshot for both
        queue = str(self.cfg("MeterValuesSkipWhenOffline", "false")).lower() != "true"   # real charger: queue while offline
        if override:
            for s in samples:
                if s["measurand"] == "Power.Active.Import":
                    s["value"] = "0.0"
        for connector in (0, 1):
            req = call.MeterValues(connector_id=connector, transaction_id=self.tx_id if connector == 1 else None,
                                   meter_value=[{"timestamp": utc_iso(stamp), "sampledValue": [dict(x) for x in samples]}])
            await self.send(req, queue=queue and not boot)
            await asyncio.sleep(0.03)

    # ---------- transactions ----------
    async def start_sequence(self, id_tag: str, connector: int = 1):
        """Preparing -> SuspendedEV -> StartTransaction -> Charging (timings from the real charger)."""
        async with self.lock:
            if self.tx_id is not None:
                return
            await asyncio.sleep(T_PREPARE)
            await self.set_status("Preparing")
            await asyncio.sleep(T_SUSPENDED)
            await self.set_status("SuspendedEV")
            await asyncio.sleep(T_START_TX)
            resp = await self.send(call.StartTransaction(connector_id=connector, id_tag=id_tag,
                                                         meter_start=int(round(self.meter_wh)), timestamp=utc_iso()))
            if resp is None:
                log("❌ StartTransaction got no response - not charging (no fake transaction id)")
                return
            status = getattr(resp.id_tag_info.get("status"), "value", resp.id_tag_info.get("status"))
            if status != "Accepted":
                log(f"❌ StartTransaction not accepted by Octopus: {resp.id_tag_info}")
                return
            self.tx_id, self.tx_id_tag = resp.transaction_id, id_tag
            self.tx_start_meter = float(int(round(self.meter_wh)))
            self.last_periodic_wall = time.time() - 8          # so the first sample lands ~interval-8 s after the start
            log(f"✅ Transaction {self.tx_id} started (meterStart={int(round(self.meter_wh))})")
            await asyncio.sleep(T_CHARGING)
            await self.set_status("Charging")
            self.periodic_task = self.spawn(self.periodic_loop())

    async def stop_sequence(self, reason="Remote"):
        """Finishing -> StopTransaction. The real charger stays in Finishing until the car is unplugged."""
        async with self.lock:
            if self.tx_id is None:
                log("ℹ️ No active transaction to stop")
                return
            await asyncio.sleep(0.1)
            if self.periodic_task:
                self.periodic_task.cancel()
                self.periodic_task = None
            await self.set_status("Finishing")
            await asyncio.sleep(0.15)
            await self.send(call.StopTransaction(meter_stop=int(round(self.meter_wh)), timestamp=utc_iso(),
                                                 transaction_id=self.tx_id, reason=reason, id_tag="",
                                                 transaction_data=[]), queue=True)
            log(f"🧾 Transaction {self.tx_id} stopped (meterStop={int(round(self.meter_wh))})")
            self.tx_id, self.tx_id_tag = None, None
            self.save_state(force=True)

    # ---------- background loops (run for the life of the app) ----------
    async def periodic_loop(self):
        await asyncio.sleep(max(1, self.meter_interval - 14))   # real charger: first sample ~46 s after "Charging"
        while self.tx_id is not None:
            self.advance_energy()
            now = time.time()
            if self.last_periodic_wall and now - self.last_periodic_wall > 1.5 * self.meter_interval:
                await self.backfill_periodic(now)
            await self.send_periodic()
            await asyncio.sleep(self.meter_interval)

    async def aligned_loop(self):
        while True:
            interval = self.aligned_interval
            if interval <= 0:
                await asyncio.sleep(30)
                continue
            now = time.time()
            boundary = (now // interval + 1) * interval
            await asyncio.sleep(boundary - now + 0.3)           # real charger sends ~0-1 s after the boundary
            self.advance_energy()
            now = time.time()
            due = (now // interval) * interval                  # latest boundary that has passed
            b = due if self.last_boundary is None else max(self.last_boundary + interval, due - 192 * interval)
            while b <= due:                                      # more than one means the process was frozen
                snap = None
                if now - b > 5:                                  # not taken just now (frozen/late): reconstruct its values
                    e, p = self.estimate_at(b, now)
                    snap = Snapshot(self, e, p)
                await self.send_aligned_pair(b - (interval if QUIRK_ALIGNED_LAG else 0), snap=snap)
                self.last_boundary = b
                b += interval

    async def heartbeat_loop(self):
        """Ticks every second so a new interval (from Boot or ChangeConfiguration) applies immediately."""
        while True:
            await asyncio.sleep(1)
            if self.ready and time.monotonic() - self.last_activity >= self.heartbeat_interval:
                await self.send(call.Heartbeat())
                self.last_activity = time.monotonic()

    def advance_energy(self):
        """Bring the register up to date. Safe to call from anywhere: after a PC sleep every loop wakes at once, so
        the loops that build samples call this first instead of relying on the 1 s energy tick having run."""
        now = time.monotonic()
        dt, self._energy_last = now - self._energy_last, now
        if dt > 30:   # normally ~1 s, so a long gap means the process was frozen (PC sleep)
            log(f"⏸️ Process was suspended for {dt:.0f}s (PC sleep?) - missed samples will be back-filled")
            frames.event("suspended", seconds=round(dt))
        if ENERGY_SOURCE == "integrate" and self.status == "Charging" and self.tx_id is not None:
            self.meter_wh += self.reported_power() * dt / 3600.0
            self._dirty = True

    async def energy_loop(self):
        """Integrates power into the lifetime register once a second while charging."""
        while True:
            await asyncio.sleep(1)
            self.advance_energy()
            if self._dirty:
                self.save_state()

    # ---------- connection lifecycle ----------
    async def boot(self):
        while True:
            resp = await self.session.call(call.BootNotification(
                charge_point_vendor=opt("vendor", "Wall Box Chargers", "charger"),
                charge_point_model=opt("model", "PLP1-0-2-2", "charger"),
                charge_point_serial_number=opt("chargepoint_serial_number", "", "charger"),
                charge_box_serial_number=opt("chargebox_serial_number", "", "charger"),
                firmware_version=opt("firmware_version", "6.7.43", "charger"),
                meter_type=opt("meter_type", "Internal NON compliant", "charger"),
                meter_serial_number="", iccid="", imsi=""))
            status = getattr(resp.status, "value", resp.status)
            if status == "Accepted":
                if resp.interval:
                    self.config["HeartbeatInterval"]["value"] = str(int(resp.interval))
                self.last_activity = time.monotonic()
                log(f"✅ BootNotification accepted (heartbeat interval {self.heartbeat_interval}s)")
                return
            log(f"⏳ BootNotification {status} - retrying in {resp.interval or 60}s")
            await asyncio.sleep(resp.interval or 60)

    async def on_connected(self):
        boot = not self.booted or BOOT_ON_RECONNECT
        if boot:
            await self.boot()
            self.booted = True
        self.ready = True
        await self.send_status(0, "Available")
        await self.send_status(1, self.status)
        if boot:
            await self.send_aligned_pair(boot=True)
        if self.outbox:
            self._kick_flush()

    async def worker(self):
        headers = {}
        if PASSWORD:
            headers["Authorization"] = "Basic " + base64.b64encode(f"{CHARGEPOINT_ID}:{PASSWORD}".encode()).decode()
        while True:
            reason = None
            try:
                log(f"🔌 Connecting to {SERVER_URL}")
                async with websockets.connect(SERVER_URL, subprotocols=["ocpp1.6"], extra_headers=headers,
                                              user_agent_header=USER_AGENT,
                                              ping_interval=WS_PING_INTERVAL or None, ping_timeout=20) as ws:
                    frames.event("connected", url=SERVER_URL, subprotocol=ws.subprotocol)
                    log(f"✅ WebSocket connected (subprotocol={ws.subprotocol})")
                    self.session = Session(CHARGEPOINT_ID, LoggingConnection(ws), self)
                    listener = asyncio.create_task(self.session.start())
                    setup = asyncio.create_task(self.on_connected())
                    try:
                        await listener
                    finally:
                        setup.cancel()
                        await asyncio.gather(setup, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
                log(f"⚠️ Connection lost ({reason}) - reconnecting in {RECONNECT_DELAY}s")
            finally:
                self.ready, self.session = False, None
                frames.event("disconnected", reason=reason)
            await asyncio.sleep(RECONNECT_DELAY)

    async def resume_after_restart(self):
        if self.tx_id is not None and self.status == "Charging":
            log(f"▶️ Resuming transaction {self.tx_id} after restart")
            self.tx_start_meter, self.last_periodic_wall = self.meter_wh, time.time()
            self.periodic_task = self.spawn(self.periodic_loop())

    # ---------- commands from Octopus that need follow-up messages ----------
    async def do_trigger(self, message: str, connector):
        await asyncio.sleep(0.2)                                  # let the CALLRESULT go out first
        if message == "BootNotification":
            await self.session.call(call.BootNotification(
                charge_point_vendor=opt("vendor", "Wall Box Chargers", "charger"),
                charge_point_model=opt("model", "PLP1-0-2-2", "charger"),
                charge_point_serial_number=opt("chargepoint_serial_number", "", "charger"),
                firmware_version=opt("firmware_version", "6.7.43", "charger"),
                meter_type=opt("meter_type", "Internal NON compliant", "charger"),
                meter_serial_number="", iccid="", imsi=""))
        elif message == "Heartbeat":
            await self.send(call.Heartbeat())
        elif message == "StatusNotification":
            if connector in (None, 0):
                await self.send_status(0, "Available")
            if connector in (None, 1):
                await self.send_status(1, self.status)
        elif message == "MeterValues":
            if connector == 0:
                await self.send_aligned_pair(time.time())
            else:
                await self.send_periodic(context="Trigger")     # context for triggered samples is unverified on the real charger


# ============================================================================
# OCPP session: one per websocket connection, only handles Octopus's commands
# ============================================================================
class Session(ChargePoint):
    def __init__(self, cp_id, connection, core: Charger):
        super().__init__(cp_id, connection)
        self.core = core

    async def call(self, payload, **kwargs):
        name = type(payload).__name__
        log(f"⬆️ {name}")
        try:
            return await asyncio.wait_for(super().call(payload, **kwargs), CALL_TIMEOUT)
        except asyncio.TimeoutError:
            log(f"❌ {name} timed out after {CALL_TIMEOUT}s")
            raise

    # NB: the library handles one incoming message at a time, so handlers must return quickly.
    # Anything that sends messages afterwards is spawned as a task (otherwise the reply never arrives).

    @on(Action.remote_start_transaction)
    async def on_remote_start(self, id_tag, connector_id=None, **kwargs):
        c = self.core
        locked_out = c.manual_stop_at and time.monotonic() - c.manual_stop_at < MANUAL_STOP_LOCKOUT_S
        if c.tx_id is not None or c.lock.locked() or locked_out:
            log("⚠️ RemoteStartTransaction rejected (already charging, starting, or manual-stop lockout)")
            return call_result.RemoteStartTransaction(status="Rejected")
        c.spawn(c.start_sequence(id_tag, connector_id or 1))
        return call_result.RemoteStartTransaction(status="Accepted")

    @on(Action.remote_stop_transaction)
    async def on_remote_stop(self, transaction_id, **kwargs):
        c = self.core
        if c.tx_id is None or transaction_id != c.tx_id:
            log(f"⚠️ RemoteStopTransaction rejected (asked for {transaction_id}, active {c.tx_id})")
            return call_result.RemoteStopTransaction(status="Rejected")
        c.spawn(c.stop_sequence())
        return call_result.RemoteStopTransaction(status="Accepted")

    @on(Action.get_configuration)
    async def on_get_configuration(self, key=None, **kwargs):
        cfg = self.core.config
        wanted = list(cfg) if not key else [k for k in key if k in cfg]
        unknown = [k for k in (key or []) if k not in cfg]
        return call_result.GetConfiguration(
            configuration_key=[{"key": k, "readonly": cfg[k]["readonly"], "value": cfg[k]["value"]} for k in wanted],
            unknown_key=unknown)

    @on(Action.change_configuration)
    async def on_change_configuration(self, key, value, **kwargs):
        entry = self.core.config.get(key)
        if entry is None:
            return call_result.ChangeConfiguration(status="NotSupported")
        if entry["readonly"]:
            return call_result.ChangeConfiguration(status="Rejected")
        entry["value"] = str(value)
        self.core._save_config_store()
        log(f"🛠️ ChangeConfiguration {key}={value} accepted")
        return call_result.ChangeConfiguration(status="Accepted")

    @on(Action.trigger_message)
    async def on_trigger_message(self, requested_message, connector_id=None, **kwargs):
        message = getattr(requested_message, "value", requested_message)
        if message not in ("BootNotification", "Heartbeat", "MeterValues", "StatusNotification"):
            return call_result.TriggerMessage(status="NotImplemented")
        self.core.spawn(self.core.do_trigger(message, connector_id))
        return call_result.TriggerMessage(status="Accepted")

    # --- smart charging: stored and logged, NOT acted on yet (we want to see what Octopus sends first) ---
    @on(Action.set_charging_profile)
    async def on_set_charging_profile(self, connector_id=None, cs_charging_profiles=None, **kwargs):
        self.core.profiles.append({"connector_id": connector_id, "profile": cs_charging_profiles})
        return call_result.SetChargingProfile(status="Accepted")

    @on(Action.clear_charging_profile)
    async def on_clear_charging_profile(self, **kwargs):
        status = "Accepted" if self.core.profiles else "Unknown"
        self.core.profiles.clear()
        return call_result.ClearChargingProfile(status=status)

    @on(Action.get_composite_schedule)
    async def on_get_composite_schedule(self, **kwargs):
        return call_result.GetCompositeSchedule(status="Rejected")

    # --- local auth list ---
    @on(Action.send_local_list)
    async def on_send_local_list(self, list_version=0, **kwargs):
        self.core.local_list_version = list_version
        return call_result.SendLocalList(status="Accepted")

    @on(Action.get_local_list_version)
    async def on_get_local_list_version(self, **kwargs):
        return call_result.GetLocalListVersion(list_version=self.core.local_list_version)

    # --- everything else: harmless, logged answers ---
    @on(Action.change_availability)
    async def on_change_availability(self, **kwargs):
        return call_result.ChangeAvailability(status="Accepted")

    @on(Action.reset)
    async def on_reset(self, **kwargs):
        log("ℹ️ Reset requested - acknowledged but NOT performed")
        return call_result.Reset(status="Accepted")

    @on(Action.unlock_connector)
    async def on_unlock_connector(self, **kwargs):
        return call_result.UnlockConnector(status="NotSupported")

    @on(Action.clear_cache)
    async def on_clear_cache(self, **kwargs):
        return call_result.ClearCache(status="Accepted")

    @on(Action.update_firmware)
    async def on_update_firmware(self, **kwargs):
        return call_result.UpdateFirmware()

    @on(Action.get_diagnostics)
    async def on_get_diagnostics(self, **kwargs):
        return call_result.GetDiagnostics()

    @on(Action.reserve_now)
    async def on_reserve_now(self, **kwargs):
        return call_result.ReserveNow(status="Rejected")

    @on(Action.cancel_reservation)
    async def on_cancel_reservation(self, **kwargs):
        return call_result.CancelReservation(status="Rejected")

    @on(Action.data_transfer)
    async def on_data_transfer(self, **kwargs):
        return call_result.DataTransfer(status="UnknownVendorId")


# ============================================================================
# FastAPI app + UI
# ============================================================================
LOOP: asyncio.AbstractEventLoop | None = None
charger: Charger | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global LOOP, charger
    LOOP = asyncio.get_running_loop()
    charger = Charger()
    mqtt_bridge.start(charger.mqtt_command)
    jobs = [asyncio.create_task(c) for c in (charger.worker(), charger.aligned_loop(),
                                             charger.heartbeat_loop(), charger.energy_loop())]
    await charger.resume_after_restart()
    log(f"🚀 Virtual charger started (energy_source={ENERGY_SOURCE})")
    try:
        yield
    finally:
        for t in jobs + list(charger.tasks):
            t.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        charger.save_state(force=True)
        mqtt_bridge.stop()
        log("🛑 Stopped")


app = FastAPI(title="Virtual Car Charger", lifespan=lifespan)

ICON_DATA_URI = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAOmklEQVR42t2ba6wd1XXHf2vPnjnn3KcdPy+28QPa0PKw02IgBhNqqYGEQlMlQFs+tahSqzQfWgk5TavYV5T0QwSK1CYBNaBQilANQQmVEZCWUkxJCkkopKaFxNgG/Lxc+/o+z2Nmr36YmXPmnHuOz8uJGkay59wzjzP/vfda67/+a43QbFMVdu8WxscdwJKHH9uueDdSia4FLhYxIzinqIggoAACKsn1giR7MntRSc6jtk/PbbhP9XqtPyZNfgtQEU9wbhqV/eL5L4SR2fv+zqv2AbBrl2H3bkXiq7ObNAFvEHEAIw/tuU1s8FkqlWtMLi9EDq2E4LQGqEPw8fedgV987wbwmetrAwtgEBuA8aG0oHjBixpFXz2x86P/FA+EGsZjbM0HYM8ej1tvjQp/+43zgiWjD0iQuwEVmF8AlSg+X0y34M/pzLcCX93jUFEBT3JDgODKpafDhek7Jsc/fjTFuHgAkgMj9z+8VYYGvyPWH9O5uUgwgHiLAP08wVfv1w58w7mOCEDywx5heEyLM799fNd1r2QHQbLLfuTvH7nCDOSfBrNUS6VQMLbuIf/fg198fXwNofELVlVPu4XZG06Mf+zl1ByEXbsMwMCGDSt9O/SaGLtSi6VIjPHqHugXFLykz+pcJMGAp2F4Mqy4zRM8dRJ2Y7j4YmF83PkUHpJcfqWWSmEVPB8E8CkMz9NyMTTBwErPuIcYH3e88ZgIwOg393xGCoXHdH4+FBVbdQ36QQAv1d9I7hxKMGwpz91y5ItbHzeoCs7diVMVJ9L0R7sAnzxS3d6kf6fHNfkuuU/CJpJz5WcJHhwiquqcuxNVkaEHH93u2eA5CUMPjPRq8x4GVahEmvk+DV/1MyqZB8uuNokjOb4xOBfzsXMKXqsrTrFBpOXiDmvEu9n4OauVKELwegU/XQ7JGY/lhVzDsm8GPvvg9bNcjpQTs2UGfYs1gDuH4FMeAc54eRtJdLM1TrcRRYBIL8veYJgth3xyw2q+cMUvc+HSoRYUM/1WWxDQeGLmKxFPvjnBl/YdpFhxWKmthL7BV/EhRCGobrOKXEYljLH0MPOz5ZArx5bxrZuvbAG6u21p3udPLl/HmqE8t3/rxwwHNn6ccwZeAIyGFYxymRHjDcXcXnpweFCOlM9t2ZR8dqhWU4Wut0gVpxA65aaLVvBrYyPMVRwGc+7AV1miA2OHbD9ZnaoQGI+xwTwKGCMkcYT/OHIKp5nFrs0NQxDKkePCpQOsG03ukxxbNZgjjGYxFqJzBj6Dxzm1faW0Ettu6DQ5F0JVdj7/Bve+cpAh30OdoE0SG8lwgUqkjA3leeK2LWxePYxTsAJhOoDaCKp/8Mn/YvvL54XMz2KNMFWq8OgbR1mWD/CNQV021GVCYIYEBXnDodNF9r71PltWD+M0Hl2LYDF4IouTVxVc5h5dzXwmMtm+GJ5k4nzmEYcDy8RcBYcmHlyr91XNmETyOYog8Ax5a+ruNVeOOLMQggqhS2KIAy/OyBkKLNrLzGew2J7BZ0lOg207V0c62tNbiYGlg5PebevaEVBhMPBikhUqFy4r8MmLlvHIj07yxH9PMujHx3oBjwq2L24vDcuKetOoublOuL1kLDPe796xsWXEuHztMM8fOMN8yeEZUw2V3YAHsP0lNtLEs2udnXUEXupNol2oROHtySLzJYeRxMn2AF7SFdBzVifS5KFrCY1IQwRow+1N+mfiBHf/yyF+8N5MYgLCQtnx1zesZ/PYEGcWQubLykjei02uB/Ag2P5S2mwMqFHahdARGMGKoBlTQGo2T2bvGwMqlEKtW0M/eG+Gvf97iiV5H1WYKTr+/No1MQUXwWQmoHPwWWWZeh/QdT4vklFk4+VZ8A2XrhjmqTdPkvNtNauTZjafDGBMqAzXrB/Neg0GA8uSvM9oLvb2Hg4rpsHaEkA9gK/zAb2JGWQ8cEyBc57hHz+1mW+8+l4ySCzK+mrOUjFiKIWO7RuWcM360RioqRI1okiJkqjiXBMeqfXJUjfg66JAT0oO9d47teGleZ87P7qx61yg6lbqwqs09a/9LPt6ItR4QTcyVlMnGAOIVM8K1Dc1YKFTjMR2vfjk2My0WS7R8F234EXB9qXhtcj8RcBK8+TYqWJE+NcDp9l3cIpLVw/x6UtW0HS8sqJKlrDVrcCUYncPPo5Y/YBXkC5S3zRNnpgr88fffpPxpw5w4NRCLb43q9tpVj5rckZdftEdeBRMz+ptVQDtXAbRZPa/9Pxh3pkq8avrRrjj8rFYXBFZXLFr8AWii02gWb2xU/D1K6BX6brl7CxmcJ4R9h06w8OvniTnGX59zQjLBnycKs0tJpPqKnVEBmhqut2ARyVZAT3r9i3dwGLvnihGn3/m7WpIu/GiZXWm0WpLV5pnpM7z2CwZ6hi81OEz/RUtMv/OZvsuXvr3/edRXj0yQ+AZ1o7kuW7TKOHZ0Gu9aHJ63hEmviJS5cx8xHxJY+oNHYKnjseY/io27RdA6vUPTxW558X3WJr3mVoIuW3zClYNBVgjWNP6DgahHCpL8j7f/P1NbFkziFO47LwBnvijX2LL2kFmSy7RCLoDn/CAPspV0sQxNeEExsDd/3aYidkKKwcCCtbj6JkK97xwhEqkLC1Y7rhiVd0Sz8hWOCfcd9tGrr1guKYeD1huvGSUqzYOsuMrP2FiJsRP6zodgk+YYD+1ujaOz8WO76k3J3n0vyZYXvApR0reeux5fQLnYHYu4t7f2YQ1Uj0/OwSVUDl/ic/l5w8SOUUkzhoVCCNl2aDl0rECeyenyRVMTJvbgc9wCNNfobK1/WtiX1PFkL965hAGKIcaF0BcTJd9Y/jMR1bwZ9euwWkj+NpvORdf2+gEPRMzxEpVlJWuwC9ShLqv0rb2AA7FE+GRV09wdLrM+UsKRE6ZKTqipIYYGMMXf/P8+lDRlA1KizBJIsM3hMsOwceRpJ8StUhLJSclNrdvWcXtW1YhAgdPlfitB/fjG2FiNuTuT6zn0rGBJkufqomlREddax+jSqYeTcfgayugj/p8c1msti0p2OrnA5NTTBcjAmPYvnGEP71mjCgJka0YQByqhMBKtdDiEvNyCtYQCyqOLsDXooXptyGpHRHWJNtTYN/b0zgXawb33LwR36T9Aa1n1/cMJ6dDfvTufOL8tA78qbmI/UeKFAKDc92Bj5lgX50Z7UmQJOagCgcmi8wVHXdcuZpLVsdL35h2NDJme3/4D4d56cAcqrD/aBGI97fef4gjUxUCz1TF0U7B16hwnazUTWdGex7sNJ7hk7MVfvjuHFvPH2bnjjXJMm4PXhUGfMPhyTLfPzhLKVTu2nscI/Dka9P8+5tzjOYSYbRL8JKGwc6bEKVpTa5dBgjw1sQCp+ZDvvKpjQwGBm2ZAC1WfVSFwDMM+HG7Yt43yT4umjiVnsCDYHoH31ymokWV4LtvTfG7H1nBtg3DhC28frMoQEZ7jFy8ojTZu0iqVahewDctjXXXk9NeD/BECJ1yfLrC+PXrEoLUHrw2FF9U40gwlDNYE7PBwEpCHxIhsVPwmRZA2zv4VMCMb+xaaIMiMDUf8gdbV7J+aa4awtrmDxlxNHIwmvd4+Hun+e4bs7x1rMzv3f8O75wKGcl5iWrcPfhkBXQP3kuKHFZi4M8dmOLqDSNEupilASwf9Fm+0a8Ca+c0PQOTcyE/PlKMq0IOCtbwkxNl9h8pUfANz+4PyXlCzjaW4DsHHytCXYI3Sdl6pug4U3QIcO8LR/jnN05VSUqrWZUOZh7gyFSFT993kMOTFUoVZbboODXvyFnDaMFijWG0YPoAn1GFARVNHq2DmZ8pRXz8wg/xuavXxFI2sY2ndUDPtObsnWzx0le+8InVjOS9agb4vZ/Occ8z72O9+DerJfiewFclNrWCJ+A6mvlixXHR8kEeuvXDDAQe53pLB2nt0oC1S4O6Y9suGGAo7/GXj59gNG/iPvi+wAuIJxbnZhFvKB2EVg7PiKFYCfnwigIDgUc5cjHDy9AhI3JOBkITJammK8QrY9umgdpvar/gDRpFs1bR140Ntmm56KRJblArYELOGt6eLOI0bmn5WW2Ng5l2zvzPsRKVSOuyvp7AK068vNFw4XUL3ksYuw0VrSNEDd1YDhjwPV4/Ns/OvQf5ix3rEOli1nvsHUyd56uHi9z15AQDQZb29gA+6VQS4yNSekmW3/38dhvkniOseLV22RataEm0WKg4Vg4Gmfp8M829sX7Q0ITdkW5fo8Pvz8SvLAVWUNcH+MT5iQkiVy7uEFRl9d+8+H3JD27VhXmHiNeqDy8FZBAqkdZ3c3dAqFqC1LOJGfE5vhervuroDzxExh8xWp555cDX11xlEFGM+bJgBBE9WxNiyrfjXNzgG0OQ7H1Pmu+rxw2BEXzP4Gf3xhB4sui6oHp9fG6qJfYHXuKgjxEx/pcRUcMte7zjn7/68Whh5lmTG7aihPWFj+Z9eGljRNoJWu0ITb9PztG0gSHpFEnb4TR9HyA9N3OfVAJLz1Ht0+bT53YaerlRG5Wmnv3p11Y9zi17PGGXGtjNCq5bafMDr4nxV2qpGCHGo88mxO7KVb3l853PvIs8O+RpFJ6syNzmwys2nGQckjcpdzMx/hvHXbF4k6qelqDgoRryAQGvTkNjhzx17nRUnrnp8Nc2Hk+uc3GEHRfHnj3eifGPvaylmetVOWbyI1ZUo/jlw19U8ESoRja3xOL0WFSZv/7gAxe8zC3qpa/QNn11dtmdz56XG/3QA2ILN6CgpTlQiUTTV2frtUA5a+GkfYm6nXTdSsxY1LabkJxEIvGMPxzne5Xi06XS5B3vPvgrR7lFPR6TqLWgl3nB+Ly7fnibiP9Zoso14g8KUYiGlaRdq5Oq0c8ZvHiIySHGR8tzKib3orryVw98fazDl6dr9Kvu9fl1469tV/RGjdy1KBeTvD5ffT2iX/DtGrbbgkcRK+p0WpD9Iv4LzrD34N+t3FcFvpumr8//HyByaDdiNLxFAAAAAElFTkSuQmCC"

HTML = """<!doctype html><html><head><meta charset="utf-8"/><title>Virtual Car Charger</title>
<link rel="icon" type="image/png" href="__ICON__"/>
<style>
body{font-family:system-ui,Segoe UI,Arial;padding:16px;background:#0f1720;color:#e6eef6}
button{padding:8px 12px;margin:4px;border-radius:6px;cursor:pointer;border:none;color:#fff;background:#2563eb}
.go{background:#16a34a}.no{background:#ef4444}.mid{background:#7c3aed}
input{padding:7px;width:110px;border-radius:6px;border:1px solid #334}
.box{background:#0b1220;padding:10px;border-radius:8px;border:1px solid #152033;font-family:monospace;white-space:pre-wrap;overflow:auto}
#state span{display:inline-block;margin-right:18px}
</style></head><body>
<h2><img src="__ICON__" height="30" style="vertical-align:middle;margin-right:8px"/>Virtual Car Charger</h2>
<div id="state" class="box"></div>
<p>
<button onclick="cmd('available')" class="go">Available</button>
<button onclick="cmd('preparing')">Preparing</button>
<button onclick="cmd('suspendedev')">Suspended EV</button>
<button onclick="cmd('finishing')" class="mid">Finishing</button>
<button onclick="cmd('charge')" class="go">Start charging</button>
<button onclick="cmd('stop')" class="no">Stop</button></p>
<p>Power (W) <input id="p" type="number" step="1"><button onclick="setp()">Set</button>
&nbsp; Lifetime meter (Wh) <input id="m" type="number" step="0.1" style="width:160px"><button onclick="setm()">Set</button></p>
<h3>Commands received from Octopus</h3><div id="cmds" class="box" style="height:170px"></div>
<h3>Log</h3><div id="logs" class="box" style="height:330px"></div>
<script>
async function post(u,b){await fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)})}
const cmd=c=>post('api/cmd',{cmd:c});
const setp=()=>post('api/power',{watts:parseFloat(document.getElementById('p').value)});
const setm=()=>post('api/meter',{wh:parseFloat(document.getElementById('m').value)});
async function poll(){try{
 const s=await (await fetch('api/state')).json();
 document.getElementById('state').innerHTML=`<span>Status: <b>${s.status}</b></span><span>Tx: ${s.transaction_id??'-'}</span><span>Power: ${s.power_w} W</span><span>Meter: ${s.meter_wh.toFixed(1)} Wh (${(s.meter_wh/1000).toFixed(3)} kWh)</span><span>Octopus: ${s.connected?'connected':'OFFLINE'}</span><span>Queued: ${s.queued}</span>`;
 const c=await (await fetch('api/commands')).json();
 document.getElementById('cmds').innerText=c.commands.map(x=>x.ts.slice(11,19)+' '+x.action+' '+JSON.stringify(x.payload)).reverse().join('\\n')||'(none yet)';
 const l=await (await fetch('api/logs')).json(); const el=document.getElementById('logs');
 const bottom=el.scrollTop+el.clientHeight>=el.scrollHeight-20; el.innerText=l.logs.join('\\n'); if(bottom) el.scrollTop=el.scrollHeight;
}catch(e){console.error(e)}}
setInterval(poll,1500);poll();
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML.replace("__ICON__", ICON_DATA_URI)


@app.post("/api/cmd")
async def api_cmd(req: Request):
    cmd = (await req.json()).get("cmd")
    if not cmd:
        return JSONResponse({"ok": False}, status_code=400)
    log(f"[UI] {cmd}")
    await charger.command(cmd)
    return {"ok": True}


@app.post("/api/power")
async def api_power(req: Request):
    try:
        charger.set_power(float((await req.json())["watts"]))
        return {"ok": True}
    except (KeyError, ValueError, TypeError):
        return JSONResponse({"ok": False}, status_code=400)


@app.post("/api/meter")
async def api_meter(req: Request):
    try:
        body = await req.json()
        charger.set_meter_wh(float(body["wh"]) if "wh" in body else float(body["kwh"]) * 1000)
        return {"ok": True}
    except (KeyError, ValueError, TypeError):
        return JSONResponse({"ok": False}, status_code=400)


@app.get("/api/state")
async def api_state():
    c = charger
    return {"status": c.status, "transaction_id": c.tx_id, "power_w": c.power_w, "meter_wh": round(c.meter_wh, 1),
            "connected": c.ready, "queued": len(c.outbox), "energy_source": ENERGY_SOURCE}


@app.get("/api/commands")
async def api_commands():
    return {"commands": list(frames.commands)}


@app.get("/api/logs")
async def api_logs():
    return {"logs": list(log_buffer)}
