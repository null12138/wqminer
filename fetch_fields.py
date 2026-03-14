#!/usr/bin/env python3
"""Fetch data fields with pagination and backoff (avoids 429)."""

import argparse
import json
import random
import time
from typing import Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth


BASE_URL = "https://api.worldquantbrain.com"


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch WorldQuant data fields with pagination")
    parser.add_argument("--credentials", default="", help="Credential JSON path")
    parser.add_argument("--username", default="", help="WorldQuant username/email")
    parser.add_argument("--password", default="", help="WorldQuant password")
    parser.add_argument("--region", default="USA")
    parser.add_argument("--universe", default="TOP3000")
    parser.add_argument("--delay", type=int, default=1)
    parser.add_argument("--categories", default="fundamental,analyst,model,news,alternative")
    parser.add_argument("--dataset-page-limit", type=int, default=50)
    parser.add_argument("--dataset-max-pages", type=int, default=3)
    parser.add_argument("--field-page-limit", type=int, default=50)
    parser.add_argument("--field-max-pages", type=int, default=5)
    parser.add_argument("--min-interval", type=float, default=0.4, help="Minimum seconds between requests")
    parser.add_argument("--jitter", type=float, default=0.2, help="Random jitter seconds")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--output", default="", help="Output fields JSON path")
    parser.add_argument("--dataset-output", default="", help="Output datasets JSON path")
    return parser.parse_args()


def load_credentials(path: str) -> tuple[str, str]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list) and len(payload) >= 2:
        return str(payload[0]), str(payload[1])
    if isinstance(payload, dict):
        if payload.get("username") and payload.get("password"):
            return str(payload["username"]), str(payload["password"])
        if payload.get("email") and payload.get("password"):
            return str(payload["email"]), str(payload["password"])
    raise ValueError("Unsupported credentials format")


def ensure_parent(path: str) -> None:
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    args = parse_args()

    if args.credentials:
        username, password = load_credentials(args.credentials)
    else:
        username, password = args.username, args.password

    if not username or not password:
        raise ValueError("Need --credentials or both --username and --password")

    region = args.region.upper()
    universe = args.universe
    delay = int(args.delay)

    output = args.output or f"data/cache/data_fields_{region}_{delay}_{universe}.json"
    dataset_output = args.dataset_output or f"data/cache/data_sets_{region}_{delay}_{universe}.json"

    sess = requests.Session()
    sess.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
    sess.auth = HTTPBasicAuth(username, password)

    last_request_ts = 0.0
    cooldown_until = 0.0
    min_interval_floor = float(args.min_interval)
    rate_limit_hits = 0
    rate_limit_window_start = 0.0

    def throttle():
        nonlocal last_request_ts, cooldown_until, min_interval_floor, rate_limit_hits, rate_limit_window_start
        now = time.monotonic()
        if rate_limit_window_start and now - rate_limit_window_start > 120:
            min_interval_floor = max(0.0, min_interval_floor * 0.5)
            rate_limit_window_start = now
            rate_limit_hits = 0

        interval = max(float(args.min_interval), float(min_interval_floor))
        wait = interval - (now - last_request_ts) if interval > 0 else 0.0
        if cooldown_until > now:
            wait = max(wait, cooldown_until - now)
        if args.jitter > 0:
            wait += random.uniform(0.0, args.jitter)
        if wait > 0:
            time.sleep(wait)
        last_request_ts = time.monotonic()

    def authenticate() -> None:
        for attempt in range(1, 6):
            throttle()
            response = sess.post(f"{BASE_URL}/authentication", timeout=args.timeout)
            if response.status_code in (200, 201):
                token = response.headers.get("X-WQB-Session-Token")
                if token:
                    sess.headers.update({"X-WQB-Session-Token": token})
                return
            if response.status_code in (429, 500, 502, 503, 504) and attempt < 5:
                sleep_with_retry_after(response, attempt)
                continue
            raise RuntimeError(f"Authentication failed: {response.status_code} {response.text}")

    def sleep_with_retry_after(response: requests.Response, attempt: int) -> None:
        nonlocal cooldown_until, min_interval_floor, rate_limit_hits, rate_limit_window_start
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            sec = max(1, int(retry_after))
        else:
            sec = min(30, 2 ** (attempt - 1))
        now = time.monotonic()
        cooldown_until = max(cooldown_until, now + sec)
        if rate_limit_window_start == 0.0 or now - rate_limit_window_start > 60:
            rate_limit_window_start = now
            rate_limit_hits = 0
        rate_limit_hits += 1
        floor = max(1.0, min(6.0, sec * (1.0 + 0.5 * rate_limit_hits)))
        if floor > min_interval_floor:
            min_interval_floor = floor
        time.sleep(sec)

    def request(method: str, path: str, max_retries: int = 5, **kwargs) -> requests.Response:
        url = f"{BASE_URL}{path}"
        last = None
        for attempt in range(1, max_retries + 1):
            throttle()
            try:
                response = sess.request(method, url, timeout=args.timeout, **kwargs)
            except requests.RequestException as exc:
                if attempt < max_retries:
                    sec = min(30, 2 ** (attempt - 1))
                    time.sleep(sec)
                    continue
                raise RuntimeError(f"Request failed: {exc}") from exc
            last = response
            if response.status_code == 401 and attempt < max_retries:
                authenticate()
                continue
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                sleep_with_retry_after(response, attempt)
                continue
            return response
        return last

    def get_datasets(category: str, page: int, limit: int) -> List[Dict]:
        params = {
            "category": category,
            "instrumentType": "EQUITY",
            "region": region,
            "universe": universe,
            "delay": delay,
            "page": page,
            "limit": limit,
        }
        resp = request("GET", "/data-sets", params=params)
        resp.raise_for_status()
        return resp.json().get("results", [])

    def get_data_fields(dataset_id: str, page: int, limit: int) -> List[Dict]:
        params = {
            "dataset.id": dataset_id,
            "instrumentType": "EQUITY",
            "region": region,
            "universe": universe,
            "delay": delay,
            "page": page,
            "limit": limit,
        }
        resp = request("GET", "/data-fields", params=params)
        resp.raise_for_status()
        return resp.json().get("results", [])

    authenticate()

    categories = [c.strip() for c in args.categories.split(",") if c.strip()]

    datasets: Dict[str, Dict] = {}
    for category in categories:
        page = 1
        last_sig = ""
        while True:
            if args.dataset_max_pages and page > args.dataset_max_pages:
                break
            batch = get_datasets(category, page, args.dataset_page_limit)
            if not batch:
                break
            ids = sorted(str(x.get("id", "")) for x in batch if x.get("id"))
            sig = "|".join(ids)
            if sig and sig == last_sig:
                break
            last_sig = sig
            for ds in batch:
                ds_id = ds.get("id")
                if ds_id:
                    datasets[str(ds_id)] = ds
            if len(batch) < args.dataset_page_limit:
                break
            page += 1

    fields: Dict[str, Dict] = {}
    dataset_list = list(datasets.values())
    for idx, ds in enumerate(dataset_list, start=1):
        ds_id = ds.get("id")
        if not ds_id:
            continue
        page = 1
        last_sig = ""
        while True:
            if args.field_max_pages and page > args.field_max_pages:
                break
            batch = get_data_fields(str(ds_id), page, args.field_page_limit)
            if not batch:
                break
            ids = sorted(str(x.get("id", "")) for x in batch if x.get("id"))
            sig = "|".join(ids)
            if sig and sig == last_sig:
                break
            last_sig = sig
            for raw in batch:
                f_id = raw.get("id")
                if f_id:
                    fields[str(f_id)] = raw
            if len(batch) < args.field_page_limit:
                break
            page += 1

    ensure_parent(output)
    with open(output, "w", encoding="utf-8") as handle:
        json.dump(list(fields.values()), handle, ensure_ascii=False, indent=2)

    ensure_parent(dataset_output)
    with open(dataset_output, "w", encoding="utf-8") as handle:
        json.dump(list(datasets.values()), handle, ensure_ascii=False, indent=2)

    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
