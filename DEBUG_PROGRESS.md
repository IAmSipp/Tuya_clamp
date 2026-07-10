# Tuya Clamp Debug Progress

## Original Problem

Build and verify a real-time pipeline:

`Tuya Device -> Tuya Cloud -> Tuya Message Service / Pulsar -> Python Consumer -> InfluxDB`

The original consumer failed during Pulsar subscription with `Pulsar error: ConnectError`.

## Project Architecture Discovered

- `consumer_example.py` is the live application entry point.
- `mq_authentication.py` builds Tuya Pulsar authentication from `TUYA_ACCESS_ID` and `TUYA_ACCESS_KEY`.
- `message_util.py` decrypts Tuya MQ messages using AES-GCM or AES-ECB.
- `tuya_cloud_check.py` was added as a separate Tuya OpenAPI diagnostic tool.
- `mock_power.py` and `mock_temp_humidity.py` only write mock data to InfluxDB.
- `del.py` deletes sample Influx data and is not part of the live pipeline.

## Files Inspected

- `.env`
- `.env.example`
- `requirements.txt`
- `README.md`
- `consumer_example.py`
- `mq_authentication.py`
- `message_util.py`
- `mock_power.py`
- `mock_temp_humidity.py`
- `del.py`

## Changes Made

### `consumer_example.py`

- Requires explicit `TUYA_PULSAR_SERVER_URL` instead of silently using Singapore fallback.
- Masks Tuya and Influx secrets in config output.
- Uses `.env` values ahead of conflicting Windows environment variables and warns about conflicts.
- Adds diagnostics:
  - `--check-config`
  - `--check-network`
  - `--probe-pulsar-endpoints`
  - `--startup-check`
  - `--test-influx`
  - `--receive-once`
- Adds reconnect/backoff for the long-running consumer.
- Uses individual acknowledge and negative-acknowledge instead of cumulative ack.
- Parses multiple Tuya payload shapes: `bizData.properties`, `status`, `data`, and `dps`.
- Logs raw DP code/value safely without secrets.
- Converts common power meter DPs into the Influx measurement `tuya_power_meter`.
- Writes Grafana-friendly tags and fields.

### `mq_authentication.py`

- Keeps Tuya's password hash algorithm.
- Sends Pulsar auth via `auth_params_string` JSON with method `auth1`, compatible with `pulsar-client 3.12.0`.

### `message_util.py`

- Adds PKCS padding handling for AES-ECB messages.
- Keeps AES-GCM verification.

### `tuya_cloud_check.py`

- Added OpenAPI token probe across known Tuya data centers.
- Added common device-list route probes.
- Added optional `--device-id` inspection route support.
- Masks tokens, Access ID, and device IDs in output.

### `.env.example`

- Added explicit `TUYA_PULSAR_SERVER_URL` with endpoint examples.
- Added optional `TUYA_DEVICE_ID`, `TUYA_DEVICE_NAME`, `TUYA_DEVICE_LOCATION`, `TUYA_LOG_RAW_EVENTS`, and `LOG_LEVEL`.

### `README.md`

- Updated entry point, diagnostics, confirmed blocker, Influx schema, run commands, and Grafana prep notes.

## Tests Performed

### Python Version

Command:

```powershell
.\.venv\Scripts\python.exe --version
```

Result: Python 3.11.9.

### Config Check

Command:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --check-config
```

Result:

- Secrets are masked.
- Config fails intentionally because `.env` does not define `TUYA_PULSAR_SERVER_URL`.
- Current `.env` still has Tuya credentials and Influx settings.

### Pulsar Startup / Endpoint Probe

Commands:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --startup-check
.\.venv\Scripts\python.exe consumer_example.py --probe-pulsar-endpoints
```

Results:

- TCP reaches the Tuya Pulsar brokers.
- All tested endpoint/env combinations failed with `Pulsar error: ConnectError`:
  - Singapore `event`, `event-test`
  - China `event`, `event-test`
  - United States `event`, `event-test`
  - Europe `event`, `event-test`
  - India `event`, `event-test`
- Changing the local auth helper did not change this outcome.

### Network Reachability

Commands:

```powershell
Resolve-DnsName mqe-sg.iotbing.com
Test-NetConnection -ComputerName mqe-sg.iotbing.com -Port 7285
```

Result:

- DNS resolves.
- TCP port 7285 succeeds.
- This rules out a basic local DNS/firewall block for the Singapore broker.

### InfluxDB Write/Query

Command:

```powershell
.\.venv\Scripts\python.exe consumer_example.py --test-influx
```

Result:

- `client.health()` reports `fail` for the cloud endpoint.
- A diagnostic point is still written and queried successfully from bucket `tuya_test_bucket`.
- Influx token/org/bucket write/query path is confirmed usable.

### Tuya OpenAPI Token Probe

Command:

```powershell
.\.venv\Scripts\python.exe tuya_cloud_check.py --token-only
```

Result:

- Token auth succeeded on multiple Tuya OpenAPI endpoints.
- China returned cross-region IP denial.
- Access ID/Key are syntactically valid enough to obtain OpenAPI tokens.

### Tuya Device List Probe

Command:

```powershell
.\.venv\Scripts\python.exe tuya_cloud_check.py --list-devices
```

Result:

Every tested device-list API on successful token endpoints returned:

```text
28841107 No permission. The data center is suspended.Please go to the cloud development platform to enable the data center.
```

No devices were discovered through API diagnostics.

### Syntax Check

Command:

```powershell
.\.venv\Scripts\python.exe -m py_compile consumer_example.py mq_authentication.py message_util.py tuya_cloud_check.py
```

Result: all compile successfully.

## Confirmed Working Components

- Python environment and dependencies are usable.
- Tuya Access ID/Key can obtain Tuya OpenAPI tokens.
- TCP to Tuya Pulsar brokers works.
- InfluxDB write and query works for bucket `tuya_test_bucket`.
- Consumer code now has safer config, parsing, writing, and reconnect behavior.

## Confirmed Blocker

Tuya Cloud project/data-center permissions are not active. The concrete API error is:

```text
28841107 No permission. The data center is suspended.Please go to the cloud development platform to enable the data center.
```

Until this is fixed in Tuya Cloud, the consumer cannot complete:

`Tuya Cloud -> Tuya Message Service / Pulsar -> Python Consumer`

The Pulsar `ConnectError` is expected while the project/data center/message access is suspended or not enabled.

## Exact Next Step

In the Tuya IoT Platform:

1. Open the Cloud project that owns Access ID `ew99********m3f8`.
2. Go to project overview / data center authorization and enable or renew the suspended data center for the region where the device is linked.
3. Confirm the physical device is linked to this same cloud project.
4. Enable/subscribe Message Service for that project.
5. Confirm whether the project uses production `event` or test `event-test`.
6. Copy the matching Pulsar endpoint into `.env` as `TUYA_PULSAR_SERVER_URL`.
7. If available, copy the device ID into `.env` as `TUYA_DEVICE_ID`.
8. Rerun:

```powershell
.\.venv\Scripts\python.exe tuya_cloud_check.py --list-devices
.\.venv\Scripts\python.exe consumer_example.py --startup-check
.\.venv\Scripts\python.exe consumer_example.py --receive-once --receive-timeout-seconds 120
```

## Grafana Prep

InfluxDB bucket: `tuya_test_bucket`

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

Example Flux query:

```flux
from(bucket: "tuya_test_bucket")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "tuya_power_meter")
  |> filter(fn: (r) => r._field == "active_power")
```

Recommended panels:

- Voltage by phase time series
- Current by phase time series
- Active power by phase/total time series
- Energy counter or daily usage panel
- Latest value table for commissioning

## Continuation Prompt

Continue debugging `C:\Users\Asus\Organized\TrueLab\Tuya_clamp`. Read `DEBUG_PROGRESS.md` first. The code now compiles and InfluxDB write/query works, but Tuya Cloud device-list APIs return `28841107 No permission. The data center is suspended`. After the Tuya Cloud project/data center is enabled and `TUYA_PULSAR_SERVER_URL` is added to `.env`, rerun `tuya_cloud_check.py --list-devices`, `consumer_example.py --startup-check`, and `consumer_example.py --receive-once --receive-timeout-seconds 120`. Then inspect raw Tuya DP logs, confirm actual DP codes/scales, adjust `DP_MAPPINGS` if needed, and verify real points exist in InfluxDB measurement `tuya_power_meter`.

## 2026-07-08 11:00 Follow-up Run

### Commands Run

```powershell
Get-ChildItem -Force
python --version
where.exe python
python -m pip list
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m pip list
python consumer_example.py
.\.venv\Scripts\python.exe consumer_example.py
.\.venv\Scripts\python.exe consumer_example.py --check-config
.\.venv\Scripts\python.exe consumer_example.py --test-influx
.\.venv\Scripts\python.exe tuya_cloud_check.py --list-devices
.\.venv\Scripts\python.exe consumer_example.py --probe-pulsar-endpoints
Resolve-DnsName mqe.tuyaus.com
Test-NetConnection mqe.tuyaus.com -Port 7285
Resolve-DnsName mqe-sg.iotbing.com
Test-NetConnection mqe-sg.iotbing.com -Port 7285
$env:TUYA_PULSAR_SERVER_URL='pulsar+ssl://mqe.tuyaus.com:7285/'; $env:TUYA_MQ_ENV='event'; .\.venv\Scripts\python.exe consumer_example.py --startup-check
.\.venv\Scripts\python.exe -m py_compile consumer_example.py mq_authentication.py message_util.py tuya_cloud_check.py
```

### New Evidence

- Plain `python` is `C:\Users\Asus\AppData\Local\Programs\Python\Python311\python.exe` and does not have `pulsar` installed.
- Running `python consumer_example.py` fails with `ModuleNotFoundError: No module named 'pulsar'` unless the venv is used or dependencies are installed globally.
- The project venv `C:\Users\Asus\Organized\TrueLab\Tuya_clamp\.venv\Scripts\python.exe` has the required dependencies.
- `.env` exists and loads from `C:\Users\Asus\Organized\TrueLab\Tuya_clamp\.env`.
- `.env` still lacks `TUYA_MQ_ENV` and `TUYA_PULSAR_SERVER_URL`.
- `.env` contains Tuya Access ID/Key and Influx URL/token/org/bucket values; secrets were masked in output.
- `consumer_example.py` with venv fails at config validation with missing `TUYA_PULSAR_SERVER_URL`.
- InfluxDB test still writes and queries successfully in bucket `tuya_test_bucket`.
- Tuya OpenAPI token auth succeeds, but device-list APIs return `28841107 No permission. The data center is suspended.Please go to the cloud development platform to enable the data center.`
- DNS resolves for `mqe.tuyaus.com` and `mqe-sg.iotbing.com`.
- TCP port 7285 succeeds for `mqe.tuyaus.com` and `mqe-sg.iotbing.com`.
- Runtime override using US endpoint reaches the broker and then fails with `Pulsar error: ConnectError` during metadata subscription.
- All tested Pulsar region/env combinations still fail with `ConnectError` after TCP connect.

### Current Status

Local code and InfluxDB path are ready, but the Tuya Cloud project/data center is externally blocked. The consumer cannot receive real Tuya events until the Tuya Cloud data center is enabled/renewed and Message Service/device linking are active for this project.

### Next Exact Action

In Tuya IoT Platform, open the Cloud project for Access ID `ew99********m3f8`, enable/renew the suspended data center, verify the device is linked to this project, and enable Message Service. Then set the confirmed endpoint in `.env` as `TUYA_PULSAR_SERVER_URL`, rerun `tuya_cloud_check.py --list-devices`, then `consumer_example.py --startup-check`, then `consumer_example.py --receive-once --receive-timeout-seconds 120`.
