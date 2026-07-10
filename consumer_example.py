import argparse
import json
import logging
import os
import socket
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pulsar
from dotenv import dotenv_values
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from message_util import decrypt_message, message_id
from mq_authentication import get_authentication


SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"
DOTENV_VALUES = dotenv_values(ENV_PATH) if ENV_PATH.exists() else {}

MQ_ENV_PROD = "event"
MQ_ENV_TEST = "event-test"

PULSAR_ENDPOINTS = {
    "sg": "pulsar+ssl://mqe-sg.iotbing.com:7285/",
    "cn": "pulsar+ssl://mqe.tuyacn.com:7285/",
    "us": "pulsar+ssl://mqe.tuyaus.com:7285/",
    "eu": "pulsar+ssl://mqe.tuyaeu.com:7285/",
    "in": "pulsar+ssl://mqe.tuyain.com:7285/",
}

MEASUREMENT = "tuya_power_meter"
TEST_MEASUREMENT = "tuya_pipeline_test"

# These are common Tuya electric meter DP conventions. Confirm exact units/scales
# from the device DP metadata or raw events before relying on dashboard values.
DP_MAPPINGS = {
    "cur_voltage": ("voltage", 0.1, "V"),
    "voltage": ("voltage", 1.0, "V"),
    "cur_current": ("current", 0.001, "A"),
    "current": ("current", 1.0, "A"),
    "cur_power": ("active_power", 0.1, "W"),
    "power": ("active_power", 1.0, "W"),
    "active_power": ("active_power", 1.0, "W"),
    "add_ele": ("energy", 0.001, "kWh"),
    "total_forward_energy": ("energy", 0.01, "kWh"),
    "energy": ("energy", 1.0, "kWh"),
    "electricity_frequency": ("frequency", 0.01, "Hz"),
    "frequency": ("frequency", 1.0, "Hz"),
    "power_factor": ("power_factor", 0.001, ""),
    "powerfactor": ("power_factor", 1.0, ""),
}

PHASE_PAYLOAD_FIELDS = {
    "electricCurrent": ("current", 0.001, "A"),
    "current": ("current", 1.0, "A"),
    "power": ("active_power", 0.1, "W"),
    "activePower": ("active_power", 1.0, "W"),
    "voltage": ("voltage", 0.1, "V"),
    "frequency": ("frequency", 0.01, "Hz"),
    "powerFactor": ("power_factor", 0.001, ""),
}

PHASE_CODES = {
    "phase_a": "A",
    "phase_b": "B",
    "phase_c": "C",
    "phasea": "A",
    "phaseb": "B",
    "phasec": "C",
}


@dataclass(frozen=True)
class Config:
    access_id: str
    access_key: str
    pulsar_server_url: str
    mq_env: str
    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str
    device_name: str
    location: str
    raw_log: bool
    log_level: str


def normalize_env_value(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def env_value(name: str, default: str = "") -> str:
    dotenv_value = DOTENV_VALUES.get(name)
    if dotenv_value not in (None, ""):
        return normalize_env_value(dotenv_value)
    return normalize_env_value(os.environ.get(name, default))


def config_warnings() -> List[str]:
    warnings = []
    for name, dotenv_value in DOTENV_VALUES.items():
        if dotenv_value in (None, ""):
            continue
        os_value = os.environ.get(name)
        if os_value and normalize_env_value(os_value) != normalize_env_value(dotenv_value):
            warnings.append(f"{name} differs between Windows environment and .env; using .env value")
        raw = str(dotenv_value)
        if raw != normalize_env_value(raw):
            warnings.append(f"{name} contains surrounding spaces or quotes; normalized at runtime")
    return warnings


def load_config() -> Config:
    return Config(
        access_id=env_value("TUYA_ACCESS_ID"),
        access_key=env_value("TUYA_ACCESS_KEY"),
        pulsar_server_url=env_value("TUYA_PULSAR_SERVER_URL"),
        mq_env=env_value("TUYA_MQ_ENV", MQ_ENV_PROD),
        influx_url=env_value("INFLUX_URL"),
        influx_token=env_value("INFLUX_TOKEN"),
        influx_org=env_value("INFLUX_ORG"),
        influx_bucket=env_value("INFLUX_BUCKET_CLAMP") or env_value("INFLUX_BUCKET"),
        device_name=env_value("TUYA_DEVICE_NAME"),
        location=env_value("TUYA_DEVICE_LOCATION"),
        raw_log=env_value("TUYA_LOG_RAW_EVENTS", "false").lower() in {"1", "true", "yes", "on"},
        log_level=env_value("LOG_LEVEL", "INFO").upper(),
    )


def mask_value(value: str, visible: int = 4) -> str:
    if not value:
        return "<missing>"
    if len(value) <= visible * 2:
        return value[0:1] + "***" + value[-1:]
    return f"{value[:visible]}********{value[-visible:]}"


def host_port_from_pulsar_url(url: str) -> Tuple[str, int]:
    without_scheme = url.replace("pulsar+ssl://", "", 1).replace("pulsar://", "", 1)
    host_port = without_scheme.strip("/")
    if ":" not in host_port:
        return host_port, 6651
    host, port_text = host_port.rsplit(":", 1)
    return host, int(port_text)


def subscription_topic(config: Config, mq_env: Optional[str] = None) -> str:
    return f"{config.access_id}/out/{mq_env or config.mq_env}"


def subscription_name(config: Config) -> str:
    return f"{config.access_id}-sub"


def validate_config(config: Config, require_pulsar_url: bool = True, require_influx: bool = True) -> List[str]:
    missing = []
    required_tuya = {
        "TUYA_ACCESS_ID": config.access_id,
        "TUYA_ACCESS_KEY": config.access_key,
        "TUYA_MQ_ENV": config.mq_env,
    }
    if require_pulsar_url:
        required_tuya["TUYA_PULSAR_SERVER_URL"] = config.pulsar_server_url

    for name, value in required_tuya.items():
        if not value:
            missing.append(name)

    if config.mq_env and config.mq_env not in {MQ_ENV_PROD, MQ_ENV_TEST}:
        missing.append("TUYA_MQ_ENV must be 'event' or 'event-test'")

    if config.pulsar_server_url and not config.pulsar_server_url.startswith("pulsar+ssl://"):
        missing.append("TUYA_PULSAR_SERVER_URL must start with pulsar+ssl://")

    if require_influx:
        required_influx = {
            "INFLUX_URL": config.influx_url,
            "INFLUX_TOKEN": config.influx_token,
            "INFLUX_ORG": config.influx_org,
            "INFLUX_BUCKET_CLAMP or INFLUX_BUCKET": config.influx_bucket,
        }
        for name, value in required_influx.items():
            if not value:
                missing.append(name)

    return missing


def print_config_summary(config: Config) -> None:
    print("Configuration summary")
    print(f".env path: {ENV_PATH}")
    print(f"Tuya access id: {mask_value(config.access_id)}")
    print(f"Tuya access key: {mask_value(config.access_key)}")
    print(f"Pulsar endpoint: {config.pulsar_server_url or '<missing>'}")
    print(f"Tuya MQ env: {config.mq_env or '<missing>'}")
    if config.access_id and config.mq_env:
        print(f"Subscription topic: {mask_value(config.access_id)}/out/{config.mq_env}")
        print(f"Subscription name: {mask_value(subscription_name(config))}")
    print(f"Influx URL: {config.influx_url or '<missing>'}")
    print(f"Influx org: {mask_value(config.influx_org)}")
    print(f"Influx bucket: {config.influx_bucket or '<missing>'}")
    print(f"Device name tag: {config.device_name or '<unset>'}")
    print(f"Location tag: {config.location or '<unset>'}")
    for warning in config_warnings():
        print(f"Warning: {warning}")


def setup_logging(config: Config) -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Tuya Pulsar MQ events and write them to InfluxDB.")
    parser.add_argument("--check-config", action="store_true", help="Validate local configuration without connecting to services.")
    parser.add_argument("--check-network", action="store_true", help="Check TCP reachability for the configured Pulsar endpoint.")
    parser.add_argument("--probe-pulsar-endpoints", action="store_true", help="Try known Tuya Pulsar endpoints and both MQ env topics.")
    parser.add_argument("--startup-check", action="store_true", help="Connect to configured Pulsar endpoint, subscribe, then exit.")
    parser.add_argument("--test-influx", action="store_true", help="Write and query a diagnostic point in InfluxDB.")
    parser.add_argument("--receive-once", action="store_true", help="Receive one Tuya event, process it, then exit.")
    parser.add_argument("--receive-timeout-seconds", type=int, default=120, help="Timeout for --receive-once. Default: 120.")
    return parser.parse_args()


def create_pulsar_client(config: Config, endpoint: Optional[str] = None) -> pulsar.Client:
    return pulsar.Client(
        endpoint or config.pulsar_server_url,
        authentication=get_authentication(config.access_id, config.access_key),
        tls_allow_insecure_connection=True,
        operation_timeout_seconds=30,
    )


def subscribe_client(
    client: pulsar.Client,
    config: Config,
    mq_env: Optional[str] = None
) -> pulsar.Consumer:

    topic = subscription_topic(config, mq_env)
    sub_name = subscription_name(config)

    logging.info(
        "Subscribing to %s/out/%s",
        mask_value(config.access_id),
        mq_env or config.mq_env
    )

    print("\n=== PULSAR DEBUG ===")
    print("Topic:", topic)
    print("Subscription:", sub_name)
    print("MQ Environment:", mq_env or config.mq_env)

    print("Access ID loaded:", bool(config.access_id))
    print("Access ID:", mask_value(config.access_id))

    print("Access Key loaded:", bool(config.access_key))
    print(
        "Access Key length:",
        len(config.access_key) if config.access_key else 0
    )

    print("====================\n")

    return client.subscribe(
        topic,
        sub_name,
        consumer_type=pulsar.ConsumerType.Failover
    )


def check_network(config: Config) -> int:
    if not config.pulsar_server_url:
        print("TUYA_PULSAR_SERVER_URL is missing")
        return 2
    host, port = host_port_from_pulsar_url(config.pulsar_server_url)
    print(f"Checking TCP reachability to {host}:{port}")
    try:
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        print("Resolved addresses: " + ", ".join(sorted({item[4][0] for item in addresses})))
        with socket.create_connection((host, port), timeout=10):
            print("TCP connection OK")
        return 0
    except OSError as exc:
        print(f"TCP connection failed: {exc}", file=sys.stderr)
        return 1


def probe_pulsar_endpoints(config: Config) -> int:
    missing = validate_config(config, require_pulsar_url=False, require_influx=False)
    if missing:
        print("Missing or invalid configuration:", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        return 2

    successes = []
    for region, endpoint in PULSAR_ENDPOINTS.items():
        for mq_env in (MQ_ENV_PROD, MQ_ENV_TEST):
            client = None
            consumer = None
            label = f"{region}:{mq_env}"
            try:
                print(f"Probing {label} {endpoint}")
                client = create_pulsar_client(config, endpoint=endpoint)
                consumer = subscribe_client(client, config, mq_env=mq_env)
                print(f"OK {label}")
                successes.append((region, endpoint, mq_env))
            except Exception as exc:
                print(f"FAIL {label}: {exc}")
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass

    if successes:
        print("Successful Tuya Pulsar configuration(s):")
        for region, endpoint, mq_env in successes:
            print(f"- region={region} endpoint={endpoint} mq_env={mq_env}")
        return 0

    print("No Tuya Pulsar endpoint/env combination subscribed successfully.", file=sys.stderr)
    print("Next checks: Tuya data center, Message Service activation, production/test subscription, and Access ID/Key project match.", file=sys.stderr)
    return 1


def create_influx_client(config: Config) -> InfluxDBClient:
    return InfluxDBClient(url=config.influx_url, token=config.influx_token, org=config.influx_org)


def test_influx(config: Config) -> int:
    missing = validate_config(config, require_pulsar_url=False, require_influx=True)
    if missing:
        print("Missing or invalid configuration:", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        return 2

    client = create_influx_client(config)
    try:
        health = client.health()
        print(f"Influx health: {health.status}")
        write_api = client.write_api(write_options=SYNCHRONOUS)
        now = datetime.now(timezone.utc)
        point = (
            Point(TEST_MEASUREMENT)
            .tag("source", "diagnostic")
            .field("value", 1.0)
            .time(now)
        )
        write_api.write(bucket=config.influx_bucket, org=config.influx_org, record=point)
        query = f'''
from(bucket: "{config.influx_bucket}")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "{TEST_MEASUREMENT}")
  |> filter(fn: (r) => r.source == "diagnostic")
  |> last()
'''
        tables = client.query_api().query(query=query, org=config.influx_org)
        records = [record for table in tables for record in table.records]
        if not records:
            print("Influx test write did not appear in query results", file=sys.stderr)
            return 1
        print(f"Influx write/query OK in bucket {config.influx_bucket}")
        return 0
    except Exception as exc:
        print(f"Influx test failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()


def numeric_value(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def decode_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return value
    return value


def parse_timestamp(payload: Dict[str, Any], biz_data: Dict[str, Any]) -> Optional[datetime]:
    for source in (biz_data, payload):
        for key in ("time", "ts", "eventTime", "createTime", "timestamp"):
            value = source.get(key)
            number = numeric_value(value)
            if number is None:
                continue
            if number > 10_000_000_000_000:
                seconds = number / 1_000_000_000
            elif number > 10_000_000_000:
                seconds = number / 1000
            else:
                seconds = number
            try:
                return datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                continue
    return None


def normalize_properties(raw_properties: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_properties, list):
        return [item for item in raw_properties if isinstance(item, dict)]
    if isinstance(raw_properties, dict):
        return [{"code": key, "value": value} for key, value in raw_properties.items()]
    return []


def extract_event(payload: Dict[str, Any]) -> Tuple[str, Optional[datetime], List[Dict[str, Any]]]:
    biz_data = payload.get("bizData") if isinstance(payload.get("bizData"), dict) else {}
    device_id = (
        biz_data.get("devId")
        or biz_data.get("devIdStr")
        or payload.get("devId")
        or payload.get("deviceId")
        or "unknown_device"
    )

    property_sources = []
    for source in (biz_data, payload):
        for key in ("properties", "status", "data", "dps"):
            if key in source:
                property_sources.append(source.get(key))

    properties: List[Dict[str, Any]] = []
    for source in property_sources:
        decoded = decode_jsonish(source)
        properties.extend(normalize_properties(decoded))

    return str(device_id), parse_timestamp(payload, biz_data), properties


def infer_phase_from_code(code: str) -> str:
    lowered = code.lower().replace("-", "_")
    for phase_code, phase in PHASE_CODES.items():
        if lowered == phase_code or lowered.endswith("_" + phase_code):
            return phase
    if lowered.endswith("_a") or lowered.endswith("a") and "phase" in lowered:
        return "A"
    if lowered.endswith("_b") or lowered.endswith("b") and "phase" in lowered:
        return "B"
    if lowered.endswith("_c") or lowered.endswith("c") and "phase" in lowered:
        return "C"
    return "total"


def convert_property(code: str, value: Any) -> List[Tuple[str, str, float, str, float]]:
    lowered = code.lower().replace("-", "_")
    decoded_value = decode_jsonish(value)
    phase = infer_phase_from_code(lowered)
    converted: List[Tuple[str, str, float, str, float]] = []

    if lowered in PHASE_CODES and isinstance(decoded_value, dict):
        phase = PHASE_CODES[lowered]
        for key, raw_value in decoded_value.items():
            mapping = PHASE_PAYLOAD_FIELDS.get(str(key))
            raw_number = numeric_value(raw_value)
            if not mapping or raw_number is None:
                continue
            field, scale, unit = mapping
            converted.append((phase, field, raw_number * scale, unit, raw_number))
        return converted

    mapping = DP_MAPPINGS.get(lowered)
    raw_number = numeric_value(decoded_value)
    if mapping and raw_number is not None:
        field, scale, unit = mapping
        converted.append((phase, field, raw_number * scale, unit, raw_number))
        return converted

    # Support property-style names such as VoltageA, CurrentB, PowerC with values
    # already reported in engineering units.
    for token, field, unit in (
        ("voltage", "voltage", "V"),
        ("current", "current", "A"),
        ("power", "active_power", "W"),
        ("frequency", "frequency", "Hz"),
        ("powerfactor", "power_factor", ""),
        ("power_factor", "power_factor", ""),
    ):
        if token in lowered and raw_number is not None:
            converted.append((phase, field, raw_number, unit, raw_number))
            return converted

    return converted


def build_points(config: Config, payload: Dict[str, Any]) -> Tuple[List[Point], int]:
    device_id, timestamp, properties = extract_event(payload)
    grouped: Dict[str, Dict[str, float]] = defaultdict(dict)
    converted_count = 0

    for prop in properties:
        code = str(prop.get("code") or prop.get("dpCode") or prop.get("name") or "")
        value = prop.get("value")
        if not code:
            continue
        logging.info("Tuya raw DP device=%s code=%s value=%s", device_id, code, value)
        for phase, field, converted_value, unit, raw_value in convert_property(code, value):
            logging.info(
                "Tuya converted DP device=%s phase=%s code=%s raw=%s field=%s value=%s unit=%s",
                device_id,
                phase,
                code,
                raw_value,
                field,
                converted_value,
                unit,
            )
            grouped[phase][field] = float(converted_value)
            converted_count += 1

    points = []
    for phase, fields in grouped.items():
        if not fields:
            continue
        point = Point(MEASUREMENT).tag("device_id", device_id).tag("phase", phase)
        if config.device_name:
            point = point.tag("device_name", config.device_name)
        if config.location:
            point = point.tag("location", config.location)
        for field, value in fields.items():
            point = point.field(field, value)
        if timestamp is not None:
            point = point.time(timestamp)
        points.append(point)

    return points, converted_count


def process_decrypted_message(config: Config, decrypted_message: str, write_api: Any) -> int:
    payload = json.loads(decrypted_message)
    if config.raw_log:
        logging.info("Tuya decrypted event: %s", json.dumps(payload, ensure_ascii=False)[:4000])

    points, converted_count = build_points(config, payload)
    if not points:
        logging.warning("No supported measurement fields found in Tuya event; converted=%s", converted_count)
        return 0

    write_api.write(bucket=config.influx_bucket, org=config.influx_org, record=points)
    logging.info("Wrote %s point(s) to InfluxDB measurement=%s bucket=%s", len(points), MEASUREMENT, config.influx_bucket)
    return len(points)


def run_consumer_session(config: Config, receive_once: bool, receive_timeout_seconds: int) -> int:
    client = None
    consumer = None
    influx_client = None
    write_api = None
    try:
        client = create_pulsar_client(config)
        consumer = subscribe_client(client, config)
        influx_client = create_influx_client(config)
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        deadline = time.monotonic() + receive_timeout_seconds if receive_once else None
        while True:
            timeout_millis = 1000 if receive_once else None
            try:
                if timeout_millis is None:
                    pulsar_message = consumer.receive()
                else:
                    pulsar_message = consumer.receive(timeout_millis=timeout_millis)
            except pulsar.Timeout:
                if deadline and time.monotonic() >= deadline:
                    logging.error("Timed out waiting for one Tuya event after %s seconds", receive_timeout_seconds)
                    return 1
                continue

            msg_id = message_id(pulsar_message.message_id())
            try:
                decrypted_message = decrypt_message(pulsar_message, config.access_key)
                process_decrypted_message(config, decrypted_message, write_api)
                consumer.acknowledge(pulsar_message)
                if receive_once:
                    return 0
            except Exception as exc:
                logging.exception("Failed to process message %s: %s", msg_id, exc)
                try:
                    consumer.negative_acknowledge(pulsar_message)
                except Exception:
                    logging.exception("Failed to negative-ack message %s", msg_id)
                if receive_once:
                    return 1
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass
        if write_api is not None:
            try:
                write_api.close()
            except Exception:
                pass
        if influx_client is not None:
            influx_client.close()
        if client is not None:
            client.close()


def startup_check(config: Config) -> int:
    missing = validate_config(config, require_pulsar_url=True, require_influx=False)
    if missing:
        print("Missing or invalid configuration:", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        return 2
    client = None
    consumer = None
    try:
        client = create_pulsar_client(config)
        consumer = subscribe_client(client, config)
        print("Startup check OK")
        return 0
    except Exception as exc:
        print(f"Failed to subscribe to Tuya Pulsar: {exc}", file=sys.stderr)
        print("Check: data center endpoint, MQ env event/event-test, Message Service activation, and Access ID/Key project match.", file=sys.stderr)
        return 1
    finally:
        if consumer is not None:
            consumer.close()
        if client is not None:
            client.close()


def run_forever(config: Config) -> int:
    missing = validate_config(config, require_pulsar_url=True, require_influx=True)
    if missing:
        print("Missing or invalid configuration:", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        return 2

    backoff = 5
    while True:
        try:
            run_consumer_session(config, receive_once=False, receive_timeout_seconds=0)
            backoff = 5
        except KeyboardInterrupt:
            logging.info("Shutdown requested")
            return 0
        except Exception as exc:
            logging.exception("Consumer session failed: %s", exc)
            logging.info("Reconnecting in %s seconds", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)


def main() -> int:
    args = parse_args()
    config = load_config()
    setup_logging(config)

    if args.check_config:
        print_config_summary(config)
        missing = validate_config(config, require_pulsar_url=True, require_influx=True)
        if missing:
            print("Missing or invalid configuration:", file=sys.stderr)
            for item in missing:
                print(f"- {item}", file=sys.stderr)
            return 2
        print("Configuration OK")
        return 0

    if args.check_network:
        return check_network(config)

    if args.probe_pulsar_endpoints:
        return probe_pulsar_endpoints(config)

    if args.test_influx:
        return test_influx(config)

    if args.startup_check:
        return startup_check(config)

    if args.receive_once:
        missing = validate_config(config, require_pulsar_url=True, require_influx=True)
        if missing:
            print("Missing or invalid configuration:", file=sys.stderr)
            for item in missing:
                print(f"- {item}", file=sys.stderr)
            return 2
        try:
            return run_consumer_session(config, receive_once=True, receive_timeout_seconds=args.receive_timeout_seconds)
        except KeyboardInterrupt:
            logging.info("Shutdown requested")
            return 130

    return run_forever(config)


if __name__ == "__main__":
    raise SystemExit(main())
