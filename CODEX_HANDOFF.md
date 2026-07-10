# Goal

Fix the current Tuya Pulsar `ConnectError` as quickly as possible. Success means `client.subscribe(...)` returns successfully, prints `SUBSCRIBE SUCCESS`, and remains connected without repeated `Connection closed with ConnectError`.

# Current Status

Local Pulsar TCP/DNS/TLS/broker reachability is not the remaining issue. The app and the minimal smoke test both connect to the Singapore broker, then fail during subscription metadata/auth with `_pulsar.ConnectError: Pulsar error: ConnectError`.

The strongest current evidence is that the same active `s5nm...pt3r` Access ID and loaded Access Key also fail Tuya OpenAPI token creation with `1004 sign invalid` for every checked OpenAPI data center. That means the current `.env` Access Key is very likely not the Access Secret belonging to the current `s5nm...pt3r` Tuya Cloud project.

# Root Causes Eliminated

- Project `.env` path is loaded from `C:\Users\Asus\Organized\TrueLab\Tuya_clamp\.env`.
- Runtime Access ID begins with `s5nm` and ends with `pt3r`.
- Runtime Access ID length is 20.
- Runtime Access Key exists and length is 32.
- Runtime strips surrounding quotes and whitespace from `.env` values.
- Runtime Access ID/Key contain no leading/trailing whitespace after normalization.
- Runtime Access ID/Key contain no quote characters after normalization.
- No Windows `TUYA_ACCESS_ID` override is present in the process environment.
- No `.vscode` directory/launch config is present.
- Active executable code has no `ew99` or `s5nm` hard-coded credential references.
- Git history string search found no committed `ew99` or `s5nm` references.
- `pulsar.AuthenticationBasic` in installed `pulsar-client` supports `auth_params_string`.
- `consumer_example.py` passes `authentication=get_authentication(config.access_id, config.access_key)` into `pulsar.Client(...)`.
- App and smoke test fail at the same broker subscription stage.

# Current Leading Hypothesis

The Access Secret in `.env` does not match the active Tuya Cloud Project whose Access ID starts with `s5nm...`. Evidence: `tuya_cloud_check.py --list-devices` uses the same `.env`-first loader and standard HMAC-SHA256 token signing, but Tuya returns `1004 sign invalid` for all checked OpenAPI endpoints. A wrong secret would also explain Pulsar broker connect followed by `ConnectError` during partition metadata/subscription auth.

# Files Inspected

- `consumer_example.py`
- `mq_authentication.py`
- `tuya_cloud_check.py`
- `.env` via masked diagnostics only
- `.env.example`
- `README.md`
- `DEBUG_PROGRESS.md`
- `.vscode` presence check only; directory is not present

# Files Changed

- `tuya_pulsar_smoke_test.py`
  - Added a minimal isolated Pulsar smoke test.
  - It loads the same config as the app, creates the same Pulsar client path through `create_pulsar_client(config)`, subscribes to the same topic/subscription, prints stage markers, and closes cleanly if subscription succeeds.
  - Why: isolate `client.subscribe(...)` from InfluxDB, device parsing, retry loops, and business logic.

- `CODEX_HANDOFF.md`
  - This handoff file.

Existing dirty files before this work, not changed by this pass as far as observed: `README.md`, `consumer_example.py`, `message_util.py`, `mq_authentication.py`, `.env.example`, `DEBUG_PROGRESS.md`, `requirements.txt`, `tuya_cloud_check.py`.

# Commands Already Run

```powershell
rg "pulsar\.Client\(|get_authentication|AuthenticationBasic|AuthenticationToken|access_id|access_key|server_url|mq_env|subscription_topic|subscription_name|subscribe_client|ew99|s5nm"
```

```powershell
$i=0; Get-Content -LiteralPath 'consumer_example.py' | ForEach-Object { $i++; if ($i -le 240 -or ($i -ge 330 -and $i -le 490) -or ($i -ge 560 -and $i -le 680)) { '{0,4}: {1}' -f $i, $_ } }
```

```powershell
$i=0; Get-Content -LiteralPath 'consumer_example.py' | ForEach-Object { $i++; if ($i -ge 240 -and $i -le 329) { '{0,4}: {1}' -f $i, $_ } }
```

```powershell
$i=0; Get-Content -LiteralPath 'mq_authentication.py' | ForEach-Object { $i++; '{0,4}: {1}' -f $i, $_ }
```

```powershell
$envPath=(Resolve-Path '.env').Path; Write-Output "ENV_PATH=$envPath"; Get-Content -LiteralPath '.env' | ForEach-Object { if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }; $parts=$_.Split('=',2); $name=$parts[0].Trim(); $value=$parts[1].Trim(); $hasQuotes=($value.StartsWith('"') -or $value.StartsWith("'")); $trimmed=$value.Trim('"').Trim("'"); $masked=$trimmed; if ($name -match 'KEY|SECRET|TOKEN') { $masked='<secret len=' + $trimmed.Length + '>' } elseif ($trimmed.Length -gt 8) { $masked=$trimmed.Substring(0,4) + '...' + $trimmed.Substring($trimmed.Length-4) }; Write-Output ("{0}={1}; raw_len={2}; unquoted_len={3}; quoted={4}; edge_ws={5}" -f $name,$masked,$value.Length,$trimmed.Length,$hasQuotes,($value -ne $value.Trim())) }
```

```powershell
& '.\.venv\Scripts\python.exe' 'consumer_example.py' --startup-check
```

```powershell
& '.\.venv\Scripts\python.exe' -c "import pulsar, inspect; print(pulsar.AuthenticationBasic); print(inspect.signature(pulsar.AuthenticationBasic)); help(pulsar.AuthenticationBasic)"
```

```powershell
Select-String -Path 'DEBUG_PROGRESS.md','README.md','.env.example','consumer_example.py','mq_authentication.py' -Pattern 'ew99|s5nm|auth1|AuthenticationBasic|TUYA_ACCESS_ID|TUYA_ACCESS_KEY|TUYA_PULSAR_SERVER_URL|TUYA_MQ_ENV' -Context 1,1
```

```powershell
& '.\.venv\Scripts\python.exe' -c "import os, sys, importlib.metadata as m, consumer_example as c; cfg=c.load_config(); q=chr(34)+chr(39); print('python_version', sys.version.split()[0]); print('pulsar_client_version', m.version('pulsar-client')); print('env_path', c.ENV_PATH); print('dotenv_access_prefix', c.DOTENV_VALUES.get('TUYA_ACCESS_ID','')[:4]); print('runtime_access_prefix', cfg.access_id[:4]); print('runtime_access_suffix', cfg.access_id[-4:]); print('access_id_len', len(cfg.access_id)); print('access_key_exists', bool(cfg.access_key)); print('access_key_len', len(cfg.access_key)); print('access_id_edge_ws', cfg.access_id != cfg.access_id.strip()); print('access_key_edge_ws', cfg.access_key != cfg.access_key.strip()); print('access_id_has_quotes', any(ch in cfg.access_id for ch in q)); print('access_key_has_quotes', any(ch in cfg.access_key for ch in q)); print('win_env_access_exists', bool(os.environ.get('TUYA_ACCESS_ID'))); print('win_env_access_differs', bool(os.environ.get('TUYA_ACCESS_ID') and os.environ.get('TUYA_ACCESS_ID') != cfg.access_id)); print('endpoint', cfg.pulsar_server_url); print('mq_env', cfg.mq_env); print('topic', c.subscription_topic(cfg)); print('subscription', c.subscription_name(cfg)); auth=c.get_authentication(cfg.access_id,cfg.access_key); print('auth_type', type(auth).__name__)"
```

```powershell
& '.\.venv\Scripts\python.exe' 'tuya_cloud_check.py' --list-devices
```

```powershell
rg "TUYA_ACCESS_ID|TUYA_ACCESS_KEY|ew99|s5nm|ACCESS_SECRET|ACCESS_KEY" . --glob '!__pycache__/**' --glob '!.git/**'
```

```powershell
if (Test-Path '.vscode') { Get-ChildItem -Recurse -Force '.vscode' | ForEach-Object { $_.FullName } } else { Write-Output '.vscode not present' }
```

```powershell
git log --all --oneline -Sew99 -- .
git log --all --oneline -Ss5nm -- .
```

```powershell
& '.\.venv\Scripts\python.exe' 'tuya_pulsar_smoke_test.py'
```

# Important Results

Startup check failed after broker connect:

```text
Connected to broker
Connection closed with ConnectError
Error Checking/Getting Partition Metadata while Subscribing on persistent://s5nmw9ankp5rm3r7pt3r/out/event -- ConnectError
Failed to subscribe to Tuya Pulsar: Pulsar error: ConnectError
```

Runtime config/auth diagnostics:

```text
python_version 3.11.9
pulsar_client_version 3.5.0
env_path C:\Users\Asus\Organized\TrueLab\Tuya_clamp\.env
dotenv_access_prefix s5nm
runtime_access_prefix s5nm
runtime_access_suffix pt3r
access_id_len 20
access_key_exists True
access_key_len 32
access_id_edge_ws False
access_key_edge_ws False
access_id_has_quotes False
access_key_has_quotes False
win_env_access_exists False
win_env_access_differs False
endpoint pulsar+ssl://mqe-sg.iotbing.com:7285/
mq_env event
topic s5nmw9ankp5rm3r7pt3r/out/event
subscription s5nmw9ankp5rm3r7pt3r-sub
auth_type AuthenticationBasic
```

Tuya OpenAPI credential check:

```text
Tuya access id: s5nm********pt3r
FAIL us: 1004 sign invalid
FAIL eu: 1004 sign invalid
FAIL cn: 1004 sign invalid
FAIL in: 1004 sign invalid
FAIL we: 1004 sign invalid
FAIL ue: 1004 sign invalid
No Tuya OpenAPI data center accepted these credentials.
```

Smoke test stage:

```text
CONFIG OK
AUTH CREATED
CLIENT CREATED
SUBSCRIBING
_pulsar.ConnectError: Pulsar error: ConnectError
```

# Current Configuration State

- Python version: 3.11.9
- `pulsar-client` version: 3.5.0
- Endpoint: `pulsar+ssl://mqe-sg.iotbing.com:7285/`
- MQ environment: `event`
- Masked Access ID: `s5nm********pt3r`
- Topic: `s5nmw9ankp5rm3r7pt3r/out/event`
- Subscription: `s5nmw9ankp5rm3r7pt3r-sub`
- Actual `.env` path: `C:\Users\Asus\Organized\TrueLab\Tuya_clamp\.env`
- Access Key: loaded, length 32, not printed

# Minimal Reproduction

```powershell
cd C:\Users\Asus\Organized\TrueLab\Tuya_clamp
& '.\.venv\Scripts\python.exe' 'tuya_pulsar_smoke_test.py'
```

Expected current failure:

```text
CONFIG OK
AUTH CREATED
CLIENT CREATED
SUBSCRIBING
_pulsar.ConnectError: Pulsar error: ConnectError
```

# Next Exact Action

1. Open Tuya Developer Platform.
2. Go to Cloud > Development > Cloud Project.
3. Open the active project whose Access ID / Client ID begins with `s5nm` and ends with `pt3r`.
4. In Project Overview or Authorization Key, copy the Access Secret / Client Secret for this exact `s5nm...pt3r` project.
5. Update only `TUYA_ACCESS_KEY` in `C:\Users\Asus\Organized\TrueLab\Tuya_clamp\.env` with that exact secret. Do not include surrounding quotes unless needed by `.env`; the app strips normal quotes either way.
6. Run:

```powershell
& '.\.venv\Scripts\python.exe' 'tuya_cloud_check.py' --list-devices
```

7. Expected first success condition: at least one region prints `OK <region>: token received (...)`. If every region still prints `1004 sign invalid`, the secret still does not match the `s5nm...pt3r` Access ID.
8. Once token auth succeeds, verify the active project data center is Singapore / Central Data Center and Message Service is enabled for production events.
9. In the same `s5nm...pt3r` project, verify Message Service / Message Queue is enabled and authorized for the linked app/device.
10. Verify the project type/environment is production. If the project is test, set `TUYA_MQ_ENV=event-test`; otherwise keep `TUYA_MQ_ENV=event`.
11. Verify the Pulsar endpoint for Singapore is still `pulsar+ssl://mqe-sg.iotbing.com:7285/`.
12. Run:

```powershell
& '.\.venv\Scripts\python.exe' 'tuya_pulsar_smoke_test.py'
```

13. Expected success: `SUBSCRIBE SUCCESS` and no repeated `Connection closed with ConnectError` during the 10-second hold.
14. Then run the app startup check:

```powershell
& '.\.venv\Scripts\python.exe' 'consumer_example.py' --startup-check
```

15. Only after subscribe succeeds, run receive-once:

```powershell
& '.\.venv\Scripts\python.exe' 'consumer_example.py' --receive-once --receive-timeout-seconds 120
```

# Questions for User

- Please confirm that the `.env` `TUYA_ACCESS_KEY` value was copied from the exact Tuya Cloud project whose Access ID starts with `s5nm` and ends with `pt3r`, not from the older `ew99...` project.
- If updating the secret does not make `tuya_cloud_check.py --list-devices` print token success, provide a screenshot of the `s5nm...pt3r` project overview showing Access ID, data center, project status, and Message Service status. Do not include the Access Secret.

# Git State

`git status --short` at handoff time:

```text
 M README.md
 M consumer_example.py
 M message_util.py
 M mq_authentication.py
?? .env.example
?? DEBUG_PROGRESS.md
?? requirements.txt
?? tuya_cloud_check.py
?? tuya_pulsar_smoke_test.py
```

Relevant diff summary:

```text
 README.md            | 137 +++++++--
 consumer_example.py  | 780 ++++++++++++++++++++++++++++++++++++++++++++++-----
 message_util.py      |  49 ++--
 mq_authentication.py |  29 +-
```

Note: `tuya_pulsar_smoke_test.py` is untracked and was added during this pass. Existing modified files were already dirty before this pass.

# Prompt for Next Agent

Continue in `C:\Users\Asus\Organized\TrueLab\Tuya_clamp` using only `& '.\.venv\Scripts\python.exe'`. Goal: make Tuya Pulsar `client.subscribe(...)` succeed. Current failure: app and `tuya_pulsar_smoke_test.py` connect to `pulsar+ssl://mqe-sg.iotbing.com:7285/`, then fail at subscribe on `persistent://s5nmw9ankp5rm3r7pt3r/out/event` with `_pulsar.ConnectError`. Proven: `.env` path is project root, Access ID is `s5nm...pt3r`, key length 32, no whitespace/quotes, no Windows env override, auth object is `AuthenticationBasic`, no `.vscode` override, no executable `ew99` hard-code. New file added: `tuya_pulsar_smoke_test.py`. Critical evidence: `& '.\.venv\Scripts\python.exe' 'tuya_cloud_check.py' --list-devices` returns `1004 sign invalid` for all OpenAPI regions, so the leading hypothesis is Access Secret mismatch for the current `s5nm...pt3r` project. Next exact step: have user replace `.env` `TUYA_ACCESS_KEY` with the Access Secret from the exact `s5nm...pt3r` Tuya Cloud Project, run `tuya_cloud_check.py --list-devices`, then run `tuya_pulsar_smoke_test.py`.
