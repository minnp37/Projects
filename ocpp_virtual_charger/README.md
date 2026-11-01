# Virtual Car Charger

A virtual **OCPP 1.6J** car charger for Home Assistant. It connects to charge point  management system backend
and behaves like a generic OCPP charger. You decide the brand of car charger, power and lifetime energy it reports,
so Home Assistant automations can drive what the charger management system backend sees.

## Features
- Mimics a real generic OCPP car charger: configuration keys, `MeterValues` (periodic and clock-aligned), start/stop sequences and timings
- Set the **charging power** and the **lifetime energy meter** from the web UI, MQTT or an automation
- Energy accumulates from power automatically, or you can supply it yourself
- Records every OCPP message, and the commands your charger management system backend sends, to a log file for debugging
- Queues transaction messages while offline and replays them after a reconnect
- Home Assistant entities through MQTT, plus a web UI in the sidebar

## Set up
1. Install the add-on and open the **Configuration** tab.
2. Enter the `server_url`, `chargepoint_id` and `password` given by charger management system backend.
3. Set `initial_meter_wh` (in **Wh**) to where you want the lifetime meter to start. It is only used the first time, when there is no saved state.
   To reset the meter later, put a value (Wh) in `meter_wh_override`. It is applied once per value, so restarts carry on from the saved reading.
4. Start the add-on and open **Virtual Car Charger** in the sidebar.
