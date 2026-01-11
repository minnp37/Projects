# virtual_charger.py
# Drop-in replacement with fixes for:
# - duplicate heartbeats
# - robust heartbeat task management
# - single BootNotification on connect/resume
# - safer task lifecycle and cleanup
# - default timeouts on outbound OCPP calls


import asyncio
import base64
import logging
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Deque, Optional
import yaml
import paho.mqtt.client as mqtt  # New: MQTT client library
import json
from pathlib import Path

STATE_FILE = Path("/data/session_state.json")


# --- MQTT CONFIGURATION ---
# Use "127.0.0.1" for your local Windows testing with Mosquitto.
# Change to "core-mosquitto" later when moving to the HA Add-on.
MQTT_HOST = "core-mosquitto" 
MQTT_PREFIX = "homeassistant"
DEVICE_ID = "virtual_charger"
BASE_TOPIC = f"ha/charger/{DEVICE_ID}"

# Global references needed for MQTT callbacks to interact with the OCPP tasks
global_cp = None
loop = None

def load_config():
# Standard path for Home Assistant Add-on options
    HA_OPTIONS_PATH = Path("/data/options.json")
    
    if HA_OPTIONS_PATH.exists():
        with open(HA_OPTIONS_PATH, 'r') as f:
            return json.load(f) # Supervisor provides JSON, not YAML
    else:
        # Keep this for your local Windows testing
        local_yaml = Path("config.yaml")
        if local_yaml.exists():
            with open(local_yaml, "r") as f:
                return yaml.safe_load(f)
        
        # Bail out if no config is found at all
        print("❌ Critical Error: No configuration file found!")
        exit(1)
    
async def load_state(self):
    """Loads the previous state from /data/session_state.json or initializes a new one."""
    try:
        if STATE_FILE.exists():
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                self.is_charging = state.get("is_charging", False)
                self.current_transaction_id = state.get("current_transaction_id")
                self.total_energy_kwh = state.get("total_energy_kwh", 0.0)
                # Use value from state file, or fallback to HA config if missing
                self.dummy_power_watts = state.get("dummy_power_watts", initial_dummy_power)
                self.meter_wh = state.get("meter_wh", 0)
                log(f"💾 State loaded from {STATE_FILE}")
        else:
            log("🆕 No state file found. Initializing fresh state.")
            # Set defaults based on the HA configuration user just saved
            self.is_charging = False
            self.current_transaction_id = None
            self.total_energy_kwh = 0.0
            self.dummy_power_watts = initial_dummy_power
            self.meter_wh = 0 
            await self.save_state() # Create the file for next time
    except Exception as e:
        log(f"⚠️ Error loading state: {e}. Using defaults.")

async def save_state(self):
    """Saves current state to the persistent /data partition."""
    try:
        state = {
            "is_charging": self.is_charging,
            "current_transaction_id": self.current_transaction_id,
            "total_energy_kwh": self.total_energy_kwh,
            "dummy_power_watts": self.dummy_power_watts,
            "meter_wh": self.meter_wh
        }
        # Ensure the directory exists (it should, but safety first)
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f)
        # log("💾 State saved") # Commented out to reduce log clutter
    except Exception as e:
        log(f"⚠️ Error saving state: {e}")

config = load_config()
# ------------------ CONFIG (edited in config.yaml) ------------------ #


# Accessing nested 'charger' group
charger_config = config.get("charger", {})

SERVER_URL = f"{charger_config.get('server_url')}{charger_config.get('chargepoint_id')}"
CHARGEPOINT_ID = charger_config.get("chargepoint_id")
PASSWORD = charger_config.get("password")

# Accessing top-level system settings
HEARTBEAT_INTERVAL = config.get("heartbeat_interval", 10)
METER_INTERVAL = config.get("meter_interval", 60)
LOG_LINES = config.get("log_lines", 500)
# Initialize power from the config value
initial_power = config.get("dummy_power_watts", 1000)

# --- MQTT CLIENT SETUP & CALLBACKS ---
mqtt_client = mqtt.Client()

def on_mqtt_connect(client, userdata, flags, rc):
    log(f"✅ Connected to MQTT Broker (Result: {rc})")
    publish_discovery()
    client.publish(f"{BASE_TOPIC}/number/power/state", str(initial_power), retain=True)
    
    client.subscribe(f"{BASE_TOPIC}/#")

def on_mqtt_message(client, userdata, msg):
    payload = msg.payload.decode()
    log(f"📩 MQTT Received: {msg.topic} -> {payload}", heartbeat=True)
    
    # 1. Handle the ON/OFF Switch
    if msg.topic == f"{BASE_TOPIC}/switch/set":
        cmd = "charge" if payload == "ON" else "stop"
        # This is the 'recommended structure' bridge:
        asyncio.run_coroutine_threadsafe(command_queue.put(cmd), loop)
    
    # 2. Handle the 'Preparing' Button
    elif msg.topic == f"{BASE_TOPIC}/button/preparing/set":
        asyncio.run_coroutine_threadsafe(command_queue.put("preparing"), loop)
        
    # 3. Handle the Power Slider
    elif msg.topic == f"{BASE_TOPIC}/number/power/set":
        try:
            new_power = float(payload)
            if global_cp:
                # Update the charger's live power setting
                global_cp.dummy_power_watts = new_power
                # Echo state back to HA so the slider doesn't jump back
                mqtt_client.publish(f"{BASE_TOPIC}/number/power/state", payload, retain=True)
                log(f"⚡ Power updated via HA: {new_power}W")
        except ValueError:
            pass

# After the function, we link it to the client:
mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_message = on_mqtt_message

def publish_discovery():
    """Publishes MQTT Discovery JSON so HA creates the entities automatically."""
    device = {
        "identifiers": [DEVICE_ID],
        "name": "OCPP Virtual Charger",
        "model": "OCPP Simulator V1",
        "manufacturer": "Custom Python"
    }

    # Entity 1: Switch (Start/Stop)
    mqtt_client.publish(f"{MQTT_PREFIX}/switch/{DEVICE_ID}/config", json.dumps({
        "name": "Charger Power",
        "command_topic": f"{BASE_TOPIC}/switch/set",
        "state_topic": f"{BASE_TOPIC}/switch/state",
        "unique_id": f"{DEVICE_ID}_switch",
        "device": device
    }), retain=True)

    # Entity 2: Preparing Button
    mqtt_client.publish(f"{MQTT_PREFIX}/button/{DEVICE_ID}_prep/config", json.dumps({
        "name": "Set Preparing State",
        "command_topic": f"{BASE_TOPIC}/button/preparing/set",
        "unique_id": f"{DEVICE_ID}_prep",
        "icon": "mdi:ev-plug-type2",
        "device": device
    }), retain=True)

    # Entity 3: Power Number Slider
    mqtt_client.publish(f"{MQTT_PREFIX}/number/{DEVICE_ID}_power/config", json.dumps({
        "name": "Charging Power",
        "command_topic": f"{BASE_TOPIC}/number/power/set",
        "state_topic": f"{BASE_TOPIC}/number/power/state",
        "min": 0, "max": 7400, "step": 100,
        "unit_of_measurement": "W",
        "unique_id": f"{DEVICE_ID}_power",
        "device": device
    }), retain=True)

    # Entity 4: Energy Sensor (Meter)
    mqtt_client.publish(f"{MQTT_PREFIX}/sensor/{DEVICE_ID}_meter/config", json.dumps({
        "name": "Energy Meter",
        "state_topic": f"{BASE_TOPIC}/sensor/meter",
        "unit_of_measurement": "kWh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "unique_id": f"{DEVICE_ID}_meter",
        "device": device
    }), retain=True)

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from ocpp.v16 import call
from ocpp.v16 import ChargePoint as cp
from ocpp.routing import on
# call_result classes we will return from handlers
from ocpp.v16.call_result import (
    RemoteStartTransaction,
    RemoteStopTransaction,
    ChangeConfiguration,
    SendLocalList,
    TriggerMessage,
    GetConfiguration,
)
from ocpp.v16.enums import (
    RegistrationStatus,
    AuthorizationStatus,
    ChargePointStatus,
    ChargePointErrorCode,
    RemoteStartStopStatus,
    Action,
)


# ------------------ LOGGING ------------------ #
logger = logging.getLogger("virtual_charger")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s:%(message)s", "%Y-%m-%dT%H:%M:%S"))
logger.handlers.clear()
logger.addHandler(handler)

# in-memory log buffer for UI
log_buffer: Deque[str] = deque(maxlen=LOG_LINES)


def log(msg: str, heartbeat=False):
    # suppress most heartbeat logs
    if heartbeat and "Heartbeat" in msg:
        return
    ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    line = f"{ts} {msg}"
    log_buffer.append(line)
    logger.info(msg)

# ------------------ OCPP Charge Point ------------------ #
class VirtualChargePoint(cp):
    def __init__(self, id_, connection, settings):
        super().__init__(id_, connection)
        self.settings = settings
        self.current_transaction_id = None
        self.is_charging = False
        self.pending_remote_start = None
        self.stop_in_progress = False
        self.heartbeat_task = None
        self.meter_task = None
        self.heartbeat_interval = HEARTBEAT_INTERVAL
        global global_cp
        global_cp = self
        # <-- Fix must be here
        self.log = log

        self.config = {
            "MeterValueSampleInterval": "60",
            "MeterValuesAlignedData": "Energy.Active.Import.Register,Power.Active.Import",
            "MeterValuesSampledData": "Power.Active.Import",
        }
        self.meter_interval = config.get("meter_interval", 60)
        self.dummy_power_watts = config.get("dummy_power_watts", 1000)

        self.log("⚠️ Not resetting persistent meter.")  # now safe

        state = load_state()
        self.meter_at_start = None
        self.meter_wh = state.get("meter_wh", 3095457)
        self.log(f"💾 Loaded persistent meter: {self.meter_wh} Wh")

        self.heartbeat_count = 0
        self.manual_stop_requested = False

    async def call(self, msg):
        """Wrap outgoing OCPP calls with a consistent timeout and logging.
        The underlying library's ``ChargePoint.call`` is awaited here with a default
        timeout so a slow CSMS won't hang our tasks indefinitely.
        """
        action_name = type(msg).__name__
        log(f"⬆️ SENT: {msg}")
        try:
            resp = await asyncio.wait_for(super().call(msg), timeout=15)
            log(f"⬇️ RESPONSE: {resp}")
            return resp
        except asyncio.TimeoutError:
            log(f"❌ TIMEOUT: Call to {action_name} timed out after 15s.")
            raise
        except Exception as e:
            log(f"❌ CALL FAILED ({action_name}): {type(e).__name__}: {e}")
            raise

    @on("*")
    async def on_any(self, **kwargs):
        log(f"⬇️ RECEIVED (generic): {kwargs}")
        return None

    async def send_heartbeats(self, interval: int):
        try:
            while True:
                await asyncio.sleep(interval)
                # mark heartbeat log as quiet (heartbeat=True) to avoid flooding UI
                await self.call(call.Heartbeat())
                self.heartbeat_count += 1
                log(f"Heartbeat #{self.heartbeat_count}", heartbeat=True)
        except asyncio.CancelledError:
            log("❤️ Heartbeat task cancelled cleanly")
        except Exception as e:
            log(f"❌ Heartbeat task stopped unexpectedly: {e}")
        finally:
            self.heartbeat_task = None  # ✅ reset so it can restart

    async def _ensure_heartbeat_running(self):
        """Start heartbeat task only if not already running."""
        if not self.heartbeat_task or self.heartbeat_task.done():
            self.heartbeat_task = asyncio.create_task(self.send_heartbeats(self.heartbeat_interval))
            self.log(f"❤️ Heartbeat task started (interval={self.heartbeat_interval}s)")
        else:
            self.log("⚠️ Heartbeat task already running, skipping startup")

    async def send_boot_notification(self):
        req = call.BootNotification(
            charge_point_vendor=self.settings.get("vendor", ""),
            charge_point_model=self.settings.get("model", ""),
            charge_point_serial_number=self.settings.get("chargepoint_serial_number", ""),
            charge_box_serial_number=self.settings.get("chargebox_serial_number", ""),
            firmware_version=self.settings.get("firmware_version", ""),
            meter_type=self.settings.get("meter_type", "Internal NON compliant"),
            meter_serial_number="",
            iccid="",
            imsi="",
        )
        try:
            resp = await self.call(req)
            interval = getattr(resp, 'interval', None)
            log(f"BootNotification response: {resp.status} (interval={interval})")
            if resp.status == RegistrationStatus.accepted:
                # Respect server-provided interval if present, else configured default
                self.heartbeat_interval = interval or HEARTBEAT_INTERVAL
                # Ensure one heartbeat task is running
                await self._ensure_heartbeat_running()
            else:
                log(f"BootNotification rejected: {resp.status}")
        except Exception as e:
            log(f"BootNotification failed: {e}")
            raise


    # ---------- Configuration handlers ----------
    @on("GetConfiguration")
    async def on_get_configuration(self, key=None):
        log(f"⬇️ RECEIVED: GetConfiguration request for key={key}")
        if key:
            keys = [k for k in self.config.keys() if k in key]
        else:
            keys = list(self.config.keys())
        config_list = [{"key": k, "readonly": False, "value": self.config[k]} for k in keys]
        return GetConfiguration(configuration_key=config_list)

    @on("ChangeConfiguration")
    async def on_change_configuration(self, key, value):
        self.config[key] = value
        return ChangeConfiguration(status="Accepted")

    @on("SendLocalList")
    async def on_send_local_list(self, list_version, update_type, local_authorization_list=None):
        return SendLocalList(status="Accepted")

    @on("TriggerMessage")
    async def on_trigger_message(self, requested_message, connector_id=None):
        return TriggerMessage(status="Accepted")

    # -------------------------------
    # RemoteStartTransaction handler
    # -------------------------------
    @on(Action.remote_start_transaction)
    async def on_remote_start_transaction(self, id_tag: str, connector_id: int = 1, **kwargs):
        if self.manual_stop_requested:
            self.log("⚠️ Ignoring RemoteStartTransaction due to recent manual stop.")
            return RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        
        self.log(f"⬇️ RECEIVED: RemoteStartTransaction for idTag={id_tag} connector={connector_id}")
        self.pending_remote_start = {"id_tag": id_tag, "connector_id": connector_id}
        resp = RemoteStartTransaction(status=RemoteStartStopStatus.accepted)

        async def delayed_start():
            await asyncio.sleep(10)
            if self.pending_remote_start:
                try:
                    await self._start_charging_after_remote(
                        self.pending_remote_start["id_tag"],
                        self.pending_remote_start["connector_id"],
                    )
                    self.pending_remote_start = None
                except Exception as e:
                    self.log(f"❌ Failed delayed auto-start: {e}")

        asyncio.create_task(delayed_start())
        if not self.heartbeat_task or self.heartbeat_task.done():
            await self._ensure_heartbeat_running()
        else:
            log("❤️ Heartbeat still running (no restart required)")

        return resp

    # -------------------------------
    # Send StatusNotification helper
    # -------------------------------
    async def send_status_notification(self, connector_id: int, status: ChargePointStatus, error_code=ChargePointErrorCode.no_error):
        ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        req = call.StatusNotification(
            connector_id=connector_id,
            error_code=error_code,
            status=status,
            timestamp=ts,
        )
        try:
            resp = await asyncio.wait_for(self.call(req), timeout=10)
            self.log(f"⬆️ Sent StatusNotification: connector={connector_id} status={status.name}")
            return resp
        except asyncio.TimeoutError:
            self.log(f"⚠️ StatusNotification timed out for {status.name}, continuing")
        except Exception as e:
            self.log(f"❌ Failed to send StatusNotification: {e}")
        ha_state = "ON" if status == ChargePointStatus.charging else "OFF"
        mqtt_client.publish(f"{BASE_TOPIC}/switch/state", ha_state, retain=True)
        return await super().send_status_notification(connector_id, status, error_code)

    # -------------------------------
    # Internal method to execute StartTransaction
    # -------------------------------
    async def _start_charging_after_remote(self, id_tag: str, connector_id: int):
        try:
            self.log("[ocpp_worker] 🔌 RemoteStartTransaction received — skipping Preparing and starting charge directly.")

            await self.send_status_notification(connector_id, ChargePointStatus.charging)
            self.log(f"[ocpp_worker] Sent Charging status for connector {connector_id}")

            ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            start_req = call.StartTransaction(
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start=self.meter_wh,
                timestamp=ts,
            )

            try:
                start_resp = await asyncio.wait_for(self.call(start_req), timeout=10)
                txid = getattr(start_resp, "transaction_id", None) or getattr(start_resp, "transactionId", None)
                self.current_transaction_id = txid or int(datetime.utcnow().timestamp())
                self.log(f"✅ StartTransaction accepted, transaction_id={self.current_transaction_id}")
            except asyncio.TimeoutError:
                self.log("⚠️ StartTransaction timed out — will retry or continue. **FORCING CHARGING START.**")
                self.current_transaction_id = int(datetime.utcnow().timestamp())
            except Exception as e:
                self.log(f"❌ StartTransaction call failed: {e}")
                return

            # Always set charging state and start meter loop
            self.is_charging = True
            self.log("⚠️ Not resetting persistent meter.")
            await self.save_state()
            # create meter loop task only if not already running
            if getattr(self, 'meter_task', None) is None or self.meter_task.done():
                self.meter_task = asyncio.create_task(self._meter_reporting_loop(connector_id))
            self.log("✅ Charging state set and meter loop STARTED.")
        except Exception as e:
            self.log(f"❌ Error in _start_charging_after_remote: {e}")

    @on(Action.remote_stop_transaction)
    async def on_remote_stop_transaction(self, transactionId: int = None, **kwargs):
        self.log(f"⬇️ RECEIVED RemoteStopTransaction call: transactionId={transactionId}, kwargs={kwargs}")

        if self.stop_in_progress:
            self.log("⚠️ StopTransaction ignored — stop already in progress")
            return RemoteStopTransaction(status=RemoteStartStopStatus.rejected)

        self.stop_in_progress = True

        try:
            if not self.is_charging:
                self.log("⚠️ Not charging, rejecting RemoteStopTransaction.")
                return RemoteStopTransaction(status=RemoteStartStopStatus.rejected)

            # --- 1) Cancel meter loop cleanly
            if self.meter_task and not self.meter_task.done():
                self.meter_task.cancel()
                try:
                    await self.meter_task
                except asyncio.CancelledError:
                    self.log("🛑 Meter task cancelled cleanly before StopTransaction.")
                except Exception as e:
                    self.log(f"⚠️ Meter task raised during cancellation: {e}")
            self.meter_task = None

            connector_id = 1

            # --- 2) Finishing status
            await self.send_status_notification(connector_id, ChargePointStatus.finishing)
            await asyncio.sleep(1.0)

            # --- 3) Resolve correct transaction id
            tx_to_stop = transactionId or self.current_transaction_id or int(datetime.now(timezone.utc).timestamp())
            self.current_transaction_id = tx_to_stop
            await self.save_state()
            self.log(f"[stop] Using txid={tx_to_stop}")

            # --- 4) StopTransaction with timeout
            ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            stop_req = call.StopTransaction(
                transaction_id=tx_to_stop,
                meter_stop=self.meter_wh,
                timestamp=ts,
                id_tag=None,
            )

            try:
                await asyncio.wait_for(self.call(stop_req), timeout=10)
                self.log(f"🧾 StopTransaction sent for transaction_id={tx_to_stop}")
            except asyncio.TimeoutError:
                self.log("⚠️ StopTransaction timed out.")
            except Exception as e:
                self.log(f"❌ StopTransaction failed: {e}")

            # --- 5) Local charging end state
            self.is_charging = False
            await self.save_state()

            await asyncio.sleep(1.0)
            await self.send_status_notification(connector_id, ChargePointStatus.suspended_evse)
            self.log("🚗 SuspendedEVSE after remote stop.")

            # --- 6) Heartbeat logic
            if not self.heartbeat_task or self.heartbeat_task.done():
                await self._ensure_heartbeat_running()
            else:
                self.log("❤️ Heartbeat already running.")

            return RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

        finally:
            # ALWAYS reset
            self.stop_in_progress = False

        



    async def start_charging(self):
        try:
            if self.pending_remote_start:
                id_tag = self.pending_remote_start.get("id_tag")
                connector_id = self.pending_remote_start.get("connector_id", 1)
                self.pending_remote_start = None
                self.log(f"[UI] Acting on pending RemoteStartTransaction: id_tag={id_tag} connector={connector_id}")
            else:
                id_tag = "ffffffffffffff7f"
                connector_id = 1
                self.log(f"[UI] No pending remote start; using default id_tag={id_tag} connector={connector_id}")

            await self.send_status_notification(connector_id, ChargePointStatus.charging)
            self.log(f"[UI] Sent StatusNotification: Charging for connector {connector_id}")

            ts = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            request = call.StartTransaction(
                connector_id=connector_id,
                id_tag=id_tag,
                meter_start = int(self.total_energy_kwh * 1000),
                timestamp=ts,
            )

            try:
                response = await asyncio.wait_for(self.call(request), timeout=10)
                txid = getattr(response, "transaction_id", None) or getattr(response, "transactionId", None)
                self.current_transaction_id = txid or int(datetime.utcnow().timestamp())
                self.log(f"[UI] ✅ StartTransaction accepted, transaction_id={self.current_transaction_id}")
            except asyncio.TimeoutError:
                self.log("[UI] ⚠️ StartTransaction timed out; **FORCING CHARGING START.**")
                self.current_transaction_id = self.current_transaction_id or int(datetime.utcnow().timestamp())
            except Exception as e:
                self.log(f"[UI] ❌ Error sending StartTransaction: {e}")
                return

            self.is_charging = True
            await self.save_state()
            if getattr(self, 'meter_task', None) is None or self.meter_task.done():
                self.meter_task = asyncio.create_task(self._meter_reporting_loop(connector_id))
            self.log("[UI] ✅ Charging state set and meter loop STARTED.")

        except Exception as e:
            self.log(f"[UI] ❌ Unexpected error in start_charging: {e}")


    async def send_meter_values(
        self, connector_id: int, power_watts: float = None,
        increment_after: bool = True, last_reported_wh: int = 0
    ) -> int:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        power = power_watts if power_watts is not None else self.dummy_power_watts

        # Increment total energy (Wh) if requested
        if increment_after:
            energy_increment_wh = int((power * self.meter_interval) / 3600.0)
            self.meter_wh += energy_increment_wh
            self.total_energy_kwh = self.meter_wh / 1000.0
            await self.save_state()

        # Compute **delta for this periodic sample**
        delta_wh = self.meter_wh - last_reported_wh
        last_reported_wh = self.meter_wh

        # --- Clock sample (instantaneous power) ---
        clock_sample = {
            "timestamp": now,
            "sampledValue": [
                {
                    "value": f"{power:.1f}",
                    "measurand": "Power.Active.Import",
                    "unit": "W",
                    "context": "Sample.Clock",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": "0",
                    "measurand": "Power.Active.Export",
                    "unit": "W",
                    "context": "Sample.Clock",
                    "location": "Outlet",
                    "format": "Raw",
                },
            ],
        }

        clock_request = call.MeterValues(
            connector_id=connector_id,
            transaction_id=self.current_transaction_id,
            meter_value=[clock_sample]
        )
        try:
            await self.call(clock_request)
            self.log(f"⬆️ Sent MeterValues (Clock): power={power:.1f} W")
        except Exception as e:
            self.log(f"❌ Failed to send MeterValues (Clock): {e}")

        # --- Periodic sample (delta energy) ---
        periodic_sample = {
            "timestamp": now,
            "sampledValue": [
                {
                    "value": f"{delta_wh:.1f}",  # <-- only the delta!
                    "measurand": "Energy.Active.Import.Register",
                    "unit": "Wh",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": f"{power:.1f}",
                    "measurand": "Power.Active.Import",
                    "unit": "W",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": "50.0",
                    "measurand": "Frequency",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": "7200",
                    "measurand": "Power.Offered",
                    "unit": "W",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": "32",
                    "measurand": "Current.Offered",
                    "unit": "A",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": "0",
                    "measurand": "Energy.Active.Export.Register",
                    "unit": "Wh",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
                {
                    "value": "0",
                    "measurand": "Power.Active.Export",
                    "unit": "W",
                    "context": "Sample.Periodic",
                    "location": "Outlet",
                    "format": "Raw",
                },
            ],
        }

        periodic_request = call.MeterValues(
            connector_id=connector_id,
            transaction_id=self.current_transaction_id,
            meter_value=[periodic_sample]
        )
        try:
            await self.call(periodic_request)
            self.log(f"⬆️ Sent MeterValues (Periodic): delta={delta_wh:.1f} Wh, power={power:.1f} W")
        except Exception as e:
            self.log(f"❌ Failed to send MeterValues (Periodic): {e}")

        return last_reported_wh




    async def _meter_reporting_loop(self, connector_id: int):
        # Remember the starting meter for this transaction
        session_start_wh = self.meter_wh
        last_reported_wh = session_start_wh

        while self.current_transaction_id is not None:
            last_reported_wh = await self.send_meter_values(
                connector_id,
                last_reported_wh=last_reported_wh,
                increment_after=True
            )
            mqtt_client.publish(f"{BASE_TOPIC}/sensor/meter", round(self.meter_wh / 1000, 3), retain=True)
            await asyncio.sleep(self.meter_interval)

        # ✅ Send one final MeterValues at the end of transaction
        if self.current_transaction_id is None:
            await self.send_meter_values(
                connector_id,
                last_reported_wh=last_reported_wh,
                increment_after=False
            )




    async def stop_charging(self):
        try:
            self.manual_stop_requested = True
            self.log("[UI] Stop requested")
            connector_id = 1

            await self.send_status_notification(connector_id, ChargePointStatus.finishing)
            self.log("[UI] Sent StatusNotification: Finishing")
            await asyncio.sleep(1)

            txid = self.current_transaction_id
            if txid:
                try:
                    req = call.StopTransaction(
                        transaction_id=txid,
                        meter_stop=self.meter_wh,
                        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        id_tag=None,
                    )
                    await self.call(req)
                    self.log(f"[UI] ✅ StopTransaction sent for transaction_id={txid}")
                except Exception as e:
                    self.log(f"[UI] ❌ Error sending StopTransaction: {e}")
                self.current_transaction_id = None
                self.is_charging = False
                await self.save_state()
            else:
                self.log("⚠️ No active transaction id to stop")

            self.total_energy_kwh = 0.0
            self.is_charging = False
            await self.save_state()

            if getattr(self, 'meter_task', None) and not self.meter_task.done():
                self.meter_task.cancel()
                try:
                    await self.meter_task
                except asyncio.CancelledError:
                    self.log("🛑 Meter reporting loop cancelled by stop_charging")
                self.meter_task = None

            self.current_transaction_id = None
            await asyncio.sleep(1)
            await self.send_status_notification(connector_id, ChargePointStatus.available)
            self.log("[UI] Connector returned to Available")
            asyncio.create_task(self._clear_manual_stop_flag()) 

        except Exception as e:
            self.log(f"[UI] ❌ Unexpected error in stop_charging: {e}")

    async def resume_after_reconnect(self):
        try:
            await self._ensure_heartbeat_running()
            connector_id = 1
            if self.is_charging:
                await self.send_status_notification(connector_id, ChargePointStatus.charging)
                if not self.meter_task or self.meter_task.done():
                    self.meter_task = asyncio.create_task(self._meter_reporting_loop(connector_id))
                    self.log("⚡ Meter loop resumed after reconnect")
            else:
                await self.send_status_notification(connector_id, ChargePointStatus.available)

        except Exception as e:
            self.log(f"⚠️ Resume after reconnect failed: {e}")

    async def save_state(self):
        try:
            state = {
                "is_charging": self.is_charging,
                "current_transaction_id": self.current_transaction_id,
                "total_energy_kwh": getattr(self, "total_energy_kwh", 0),
                "dummy_power_watts": getattr(self, "dummy_power_watts", 1000),
                "meter_wh": int(self.meter_wh),        # <-- NEW PERSISTED FIELD
            }
            STATE_FILE.write_text(json.dumps(state))
            self.log(f"💾 State saved: {state}")
        except Exception as e:
            self.log(f"⚠️ Could not save state: {e}")

    async def load_state(self):
        if not STATE_FILE.exists():
            self.log("ℹ️ No saved session state found.")
            return

        try:
            data = json.loads(STATE_FILE.read_text())
            self.is_charging = data.get("is_charging", False)
            self.current_transaction_id = data.get("current_transaction_id")
            self.total_energy_kwh = data.get("total_energy_kwh", 0)
            self.dummy_power_watts = data.get("dummy_power_watts", 1000)

            # NEW FIELD HERE
            self.meter_wh = data.get("meter_wh", 4066494)  # fallback to real initial

            self.log(f"📂 State loaded: {data}")
        except Exception as e:
            self.log(f"⚠️ Could not load state: {e}")

    async def _clear_manual_stop_flag(self, delay: int = 60):
        await asyncio.sleep(delay)
        self.manual_stop_requested = False
        self.log("🟢 Manual stop flag cleared; future RemoteStart requests will be accepted.")

# ------------------ FastAPI app + UI ------------------ #
app = FastAPI(title="Virtual Wallbox Charger UI")
command_queue: asyncio.Queue = asyncio.Queue()
logs_lock = asyncio.Lock()

HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Virtual Wallbox Charger</title>
  <style>
    body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial; padding: 20px; background:#0f1720; color:#e6eef6; }
    button { padding:8px 12px; margin:6px; border-radius:6px; cursor:pointer; border:none; }
    .available { background:#16a34a; color:white }
    .preparing { background:#2563eb; color:white }
    .finishing { background:#7c3aed; color:white }
    .stop { background:#ef4444; color:white }
    .charge { background:#f59e0b; color:white }
    #logs { background:#0b1220; padding:12px; height:420px; overflow:auto; border-radius:8px; border:1px solid #152033; font-family:monospace; white-space:pre-wrap; }
  </style>
</head>
<body>
  <h1>🚗 Virtual Wallbox Charger</h1>
  <div>
    <button class="available" onclick="send('available')">Available</button>
    <button class="preparing" onclick="send('preparing')">Preparing</button>
    <button class="finishing" onclick="send('finishing')">Finishing</button>
    <button class="stop" onclick="send('stop')">Stop</button>
    <button class="charge" onclick="send('charge')">Charge</button>
    <button onclick="send('exit')">Exit</button>
  </div>
  <h3>Logs</h3>
  <div id="logs">Loading...</div>

<script>
async function send(cmd) {
  await fetch('/api/cmd', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({cmd})
  });
}

async function poll() {
  try {
    const r = await fetch('/api/logs');
    const j = await r.json();
    const el = document.getElementById('logs');
    el.innerText = j.logs.join('\\n');
    el.scrollTop = el.scrollHeight;
  } catch(e) {
    console.error(e);
  }
}

setInterval(poll, 1200);
poll();
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML

@app.post("/api/cmd")
async def api_cmd(req: Request):
    data = await req.json()
    cmd = data.get("cmd")
    if cmd:
        await command_queue.put(cmd)
        log(f"[UI] Enqueued command: {cmd}")
        return JSONResponse({"ok": True})
    return JSONResponse({"ok": False}, status_code=400)

@app.get("/api/logs")
async def api_logs():
    return {"logs": list(log_buffer)}





async def _run_command_processing(cp_obj, command_queue):
    cp_obj.log("Starting command processing task.")
    while True:
        try:
            cmd = await asyncio.wait_for(command_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        cp_obj.log(f"[ocpp_worker] Processing command: {cmd}")
        c = cmd.lower()
        if c == "exit":
            cp_obj.log("[ocpp_worker] Exit command received. Shutting down worker.")
            return
        elif c in ("available", "preparing", "finishing", "faulted"):
            mapping = {
                "available": ChargePointStatus.available,
                "preparing": ChargePointStatus.preparing,
                "finishing": ChargePointStatus.finishing,
                "faulted": ChargePointStatus.faulted,
            }
            await cp_obj.send_status_notification(1, mapping[c])
        elif c == "stop":
            await cp_obj.stop_charging()
        elif c == "charge":
            await cp_obj.start_charging()
        else:
            cp_obj.log(f"Unknown command: {cmd}")
        command_queue.task_done()

# ------------------ OCPP worker / reconnection loop ------------------ #
async def ocpp_worker():
    base = SERVER_URL.rstrip("/")
    if not base.endswith("/ocpp"):
        base = base + "/ocpp"
    full_url = f"{base}/{CHARGEPOINT_ID}"

    auth = base64.b64encode(f"{CHARGEPOINT_ID}:{PASSWORD}".encode()).decode()
    extra_headers = {
        "Authorization": f"Basic {auth}",
        "User-Agent": "VirtualWallbox/1.0",
        "Host": full_url.split("/")[2],
    }
    global loop
    loop = asyncio.get_running_loop()

    try:
        mqtt_client.connect(MQTT_HOST, 1883, 60)
        mqtt_client.loop_start()
        log("📡 MQTT Background Loop Started")
    except Exception as e:
        log(f"❌ Failed to connect to MQTT: {e}")

    while True:
        try:
            log(f"🔌 Connecting to OCPP central system at {full_url}")
            async with websockets.connect(
                full_url,
                subprotocols=["ocpp1.6"],
                extra_headers=extra_headers,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                log("✅ WebSocket connected")

                cp_obj = VirtualChargePoint(CHARGEPOINT_ID, ws, charger_config)
                # start the ocpp listener
                listener_task = asyncio.create_task(cp_obj.start())

                # load previous state and resume (won't re-send BootNotification here)
                await cp_obj.load_state()
                await cp_obj.resume_after_reconnect()

                # Send BootNotification only if this is a cold start (no saved heartbeat)
                if getattr(cp_obj, "heartbeat_task", None) is None or cp_obj.heartbeat_task.done():
                    log("🔌 Sending BootNotification (first startup or heartbeat missing)")
                    await cp_obj.send_boot_notification()
                else:
                    log("🔁 Skipping BootNotification on reconnect — heartbeat already active")
                    await cp_obj._ensure_heartbeat_running()

                try:
                    command_processing_task = asyncio.create_task(_run_command_processing(cp_obj, command_queue))
                    # Wait until either the listener exits (disconnect) or command task returns (exit)
                    await asyncio.gather(listener_task, command_processing_task)
                except Exception as e:
                    log(f"⚠️ Runtime task failed: {type(e).__name__}: {e}")
                finally:
                    log("Cleanup: Connection broken or runtime complete. Cancelling all CP tasks.")
                    tasks_to_cancel = [listener_task]
                    if 'command_processing_task' in locals() and command_processing_task:
                        tasks_to_cancel.append(command_processing_task)
                    hb = getattr(cp_obj, 'heartbeat_task', None)
                    if hb:
                        tasks_to_cancel.append(hb)
                    mt = getattr(cp_obj, 'meter_task', None)
                    if mt:
                        tasks_to_cancel.append(mt)

                    for task in tasks_to_cancel:
                        if task and not task.done():
                            task.cancel()

                    await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                    log("Cleanup complete. Exiting `with ws:` block to attempt reconnect.")

        except (OSError, ConnectionClosed, ConnectionClosedError, ConnectionClosedOK) as e:
            log(f"⚠️ Connection lost ({type(e).__name__}: {e}) — attempting reconnect in 10 s")
            await asyncio.sleep(10)
            continue

        except Exception as e:
            if "BootNotification" in str(e):
                log(f"❌ BootNotification error was fatal: {e}")
            else:
                log(f"❌ Unexpected non-connection error in worker: {e}")
            log("Reconnecting in 15 s...")
            await asyncio.sleep(15)
            continue

# ------------------ Lifespan (startup/shutdown) ------------------ #
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(ocpp_worker())
    log("OCPP worker started")
    try:
        yield
    finally:
        log("Shutting down: requesting exit to ocpp worker")
        try:
            await command_queue.put("exit")
        except Exception:
            pass
        await asyncio.sleep(1)
        task.cancel()
        log("Shutdown complete")

app.router.lifespan_context = lifespan

# ------------------ Run as script (optional) ------------------ #
if __name__ == "__main__":
    import uvicorn
    log("Starting FastAPI app (uvicorn)...")
    uvicorn.run("virtual_charger:app", host="0.0.0.0", port=8000, log_level="info")
