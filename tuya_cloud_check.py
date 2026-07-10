import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import dotenv_values

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"
DOTENV_VALUES = dotenv_values(ENV_PATH) if ENV_PATH.exists() else {}

API_ENDPOINTS = {
    "us": "https://openapi.tuyaus.com",
    "eu": "https://openapi.tuyaeu.com",
    "cn": "https://openapi.tuyacn.com",
    "in": "https://openapi.tuyain.com",
    "we": "https://openapi-weaz.tuyaeu.com",
    "ue": "https://openapi-ueaz.tuyaus.com",
}

DEVICE_LIST_PATHS = [
    "/v2.0/cloud/thing/device?page_no=1&page_size=20",
    "/v2.0/cloud/thing/device?pageNo=1&pageSize=20",
    "/v1.3/iot-03/devices?page_no=1&page_size=20",
    "/v1.0/iot-03/devices?page_no=1&page_size=20",
    "/v1.0/devices?page_no=1&page_size=20",
]


def normalize(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        cleaned = cleaned[1:-1].strip()
    return cleaned


def env_value(name: str) -> str:
    dotenv_value = DOTENV_VALUES.get(name)
    if dotenv_value not in (None, ""):
        return normalize(dotenv_value)
    return normalize(os.environ.get(name))


def mask(value: str, visible: int = 4) -> str:
    if not value:
        return "<missing>"
    if len(value) <= visible * 2:
        return value[:1] + "***" + value[-1:]
    return f"{value[:visible]}********{value[-visible:]}"


def make_string_to_sign(method: str, path_with_query: str, body: bytes = b"") -> str:
    content_hash = hashlib.sha256(body).hexdigest()
    return f"{method}\n{content_hash}\n\n{path_with_query}"


def sign(access_id: str, access_key: str, method: str, path_with_query: str, body: bytes = b"", token: str = "") -> Tuple[str, str]:
    timestamp = str(int(time.time() * 1000))
    sign_source = access_id + token + timestamp + make_string_to_sign(method, path_with_query, body)
    signature = hmac.new(access_key.encode("utf-8"), sign_source.encode("utf-8"), hashlib.sha256).hexdigest().upper()
    return timestamp, signature


def request_json(endpoint: str, access_id: str, access_key: str, method: str, path_with_query: str, token: str = "") -> Dict[str, Any]:
    body = b""
    timestamp, signature = sign(access_id, access_key, method, path_with_query, body=body, token=token)
    headers = {
        "client_id": access_id,
        "sign": signature,
        "t": timestamp,
        "sign_method": "HMAC-SHA256",
    }
    if token:
        headers["access_token"] = token
    req = urllib.request.Request(endpoint + path_with_query, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = {"success": False, "http_status": exc.code, "raw": text[:500]}
        data.setdefault("http_status", exc.code)
        return data


def token_probe(access_id: str, access_key: str) -> Dict[str, Dict[str, Any]]:
    results = {}
    for region, endpoint in API_ENDPOINTS.items():
        path = "/v1.0/token?grant_type=1"
        try:
            data = request_json(endpoint, access_id, access_key, "GET", path)
        except Exception as exc:
            data = {"success": False, "error": str(exc)}
        results[region] = data
    return results


def successful_tokens(results: Dict[str, Dict[str, Any]]) -> List[Tuple[str, str, str]]:
    successes = []
    for region, data in results.items():
        if data.get("success"):
            token = data.get("result", {}).get("access_token", "")
            if token:
                successes.append((region, API_ENDPOINTS[region], token))
    return successes


def print_token_results(results: Dict[str, Dict[str, Any]]) -> None:
    for region, data in results.items():
        ok = bool(data.get("success"))
        if ok:
            token = data.get("result", {}).get("access_token", "")
            print(f"OK {region}: token received ({mask(token)})")
        else:
            code = data.get("code") or data.get("http_status") or "error"
            msg = data.get("msg") or data.get("error") or data.get("raw") or "request failed"
            print(f"FAIL {region}: {code} {msg}")


def result_items(result: Any) -> List[Dict[str, Any]]:
    if isinstance(result, list):
        return [item for item in result if isinstance(item, dict)]
    if not isinstance(result, dict):
        return []
    for key in ("list", "devices", "data", "items", "records"):
        value = result.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def summarize_device(item: Dict[str, Any]) -> str:
    device_id = str(item.get("id") or item.get("dev_id") or item.get("device_id") or item.get("devId") or "")
    name = item.get("name") or item.get("device_name") or item.get("custom_name") or "<unnamed>"
    online = item.get("online") if "online" in item else item.get("is_online")
    product = item.get("product_id") or item.get("productId") or item.get("product_name") or "<unknown_product>"
    return f"device_id={mask(device_id)} name={name} online={online} product={product}"


def try_list_devices(access_id: str, access_key: str, token_rows: List[Tuple[str, str, str]]) -> bool:
    found = False
    for region, endpoint, token in token_rows:
        print(f"\nDevice list checks for region={region} endpoint={endpoint}")
        for path in DEVICE_LIST_PATHS:
            try:
                data = request_json(endpoint, access_id, access_key, "GET", path, token=token)
            except Exception as exc:
                print(f"FAIL {path}: {exc}")
                continue
            if not data.get("success"):
                code = data.get("code") or data.get("http_status") or "error"
                msg = data.get("msg") or data.get("raw") or "request failed"
                print(f"FAIL {path}: {code} {msg}")
                continue
            items = result_items(data.get("result"))
            print(f"OK {path}: {len(items)} device(s)")
            for item in items[:20]:
                print("  " + summarize_device(item))
            if items:
                found = True
    return found


def inspect_device(access_id: str, access_key: str, token_rows: List[Tuple[str, str, str]], device_id: str) -> bool:
    paths = [
        f"/v1.0/devices/{urllib.parse.quote(device_id)}",
        f"/v1.0/devices/{urllib.parse.quote(device_id)}/status",
        f"/v1.0/devices/{urllib.parse.quote(device_id)}/functions",
        f"/v1.0/devices/{urllib.parse.quote(device_id)}/specifications",
    ]
    found = False
    for region, endpoint, token in token_rows:
        print(f"\nDevice detail checks for region={region} endpoint={endpoint} device_id={mask(device_id)}")
        for path in paths:
            try:
                data = request_json(endpoint, access_id, access_key, "GET", path, token=token)
            except Exception as exc:
                print(f"FAIL {path}: {exc}")
                continue
            if not data.get("success"):
                code = data.get("code") or data.get("http_status") or "error"
                msg = data.get("msg") or data.get("raw") or "request failed"
                print(f"FAIL {path}: {code} {msg}")
                continue
            found = True
            result = data.get("result")
            print(f"OK {path}")
            print(json.dumps(result, ensure_ascii=False, indent=2)[:4000])
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose Tuya OpenAPI credentials, data center, and device metadata.")
    parser.add_argument("--token-only", action="store_true", help="Only test token authentication across known API endpoints.")
    parser.add_argument("--list-devices", action="store_true", help="Try common device-list APIs on successful data centers.")
    parser.add_argument("--device-id", default=env_value("TUYA_DEVICE_ID"), help="Inspect one known Tuya device ID.")
    args = parser.parse_args()

    access_id = env_value("TUYA_ACCESS_ID")
    access_key = env_value("TUYA_ACCESS_KEY")
    if not access_id or not access_key:
        print("TUYA_ACCESS_ID and TUYA_ACCESS_KEY are required", file=sys.stderr)
        return 2

    print(f"Tuya access id: {mask(access_id)}")
    results = token_probe(access_id, access_key)
    print_token_results(results)
    tokens = successful_tokens(results)
    if not tokens:
        print("No Tuya OpenAPI data center accepted these credentials.", file=sys.stderr)
        print("Check Access ID/Secret, project status, and whether the credentials belong to this Tuya Cloud project.", file=sys.stderr)
        return 1

    region, endpoint, _token = tokens[0]
    print(f"First successful OpenAPI endpoint: region={region} endpoint={endpoint}")
    if args.token_only:
        return 0

    any_device = False
    if args.list_devices:
        any_device = try_list_devices(access_id, access_key, tokens)
    if args.device_id:
        any_device = inspect_device(access_id, access_key, tokens, args.device_id) or any_device

    if args.list_devices and not any_device:
        print("No devices were discovered through the tested API paths.", file=sys.stderr)
        print("If the Tuya console shows a linked device, copy its Device ID into TUYA_DEVICE_ID and rerun with --device-id.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
