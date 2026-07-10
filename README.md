# Tuya clamp consumer

Python consumer for Tuya Pulsar MQ messages. It decrypts real-time Tuya device events and writes electrical measurements to InfluxDB for later Grafana dashboards.

## Entry point

`consumer_example.py` is the live application entry point. The mock scripts only generate sample InfluxDB data.

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Configure

Copy `.env.example` to `.env` and fill in the values from one Tuya Cloud project and one InfluxDB org/bucket.

Required Tuya values:

- `TUYA_ACCESS_ID`
- `TUYA_ACCESS_KEY`
- `TUYA_MQ_ENV`: `event` for production, `event-test` for test projects
- `TUYA_PULSAR_SERVER_URL`: must match the enabled Tuya data center

Required InfluxDB values:

- `INFLUX_URL`
- `INFLUX_TOKEN`
- `INFLUX_ORG`
- `INFLUX_BUCKET_CLAMP` or legacy `INFLUX_BUCKET`

## Diagnostics

Validate local config without connecting to services:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --check-config
```

Verify InfluxDB write and query:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --test-influx
```

Probe Tuya OpenAPI credentials, data centers, and device-list permissions:

```powershell
.\.venv\Scripts\python.exe tuya_cloud_check.py --list-devices
```

Probe known Tuya Pulsar endpoints and `event`/`event-test` topics:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --probe-pulsar-endpoints
```

Connect to the configured Pulsar endpoint and subscribe, then exit:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --startup-check
```

Receive one real event, write it to InfluxDB, then exit:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --receive-once --receive-timeout-seconds 120
```

Run the long-lived consumer:

```powershell
.\.venv\Scripts\python.exe consumer_example.py
```

## Current confirmed blocker

On 2026-07-08, Tuya OpenAPI token authentication succeeded, but all tested device-list APIs returned:

`28841107 No permission. The data center is suspended. Please go to the cloud development platform to enable the data center.`

Until the Tuya Cloud data center/project is enabled and the device is linked to that project, Pulsar subscription is expected to fail with `Pulsar error: ConnectError` after TCP connection.

## InfluxDB schema

Measurement: `tuya_power_meter`

Tags:

- `device_id`
- `phase`
- `device_name` when configured
- `location` when configured

Fields:

- `voltage`
- `current`
- `active_power`
- `energy`
- `frequency`
- `power_factor`

Diagnostic measurement: `tuya_pipeline_test`

Example Flux query for Grafana:

```flux
from(bucket: "tuya_test_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "tuya_power_meter")
  |> filter(fn: (r) => r._field == "voltage")
```

Recommended Grafana panels:

- Stat or time series for voltage per phase
- Time series for current per phase
- Time series for active power per phase/total
- Energy total or daily increase panel
- Table panel for latest raw troubleshooting values from logs during commissioning
