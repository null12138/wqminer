"""WorldQuant Brain API client."""

import json
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth

from .models import DataField, SimulationResult, SimulationSettings

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float = 0.0) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except Exception:
        return default


class WorldQuantBrainClient:
    BASE_URL = "https://api.worldquantbrain.com"
    PLATFORM_ALPHA_URL = "https://platform.worldquantbrain.com/alpha/"
    _global_lock = threading.Lock()
    _global_last_request_ts = 0.0
    _global_last_meta_ts = 0.0
    _global_cooldown_until = 0.0
    _global_min_interval_floor = 0.0
    _global_last_rate_limit_ts = 0.0
    _global_rate_limit_hits = 0
    _global_rate_limit_window_start = 0.0

    def __init__(self, username: str, password: str, timeout_sec: int = 30, base_url: Optional[str] = None):
        self.username = username
        self.password = password
        self.timeout_sec = timeout_sec
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.sess = requests.Session()
        self.sess.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self.sess.auth = HTTPBasicAuth(username, password)
        self.min_request_interval_sec = _env_float("WQMINER_MIN_REQUEST_INTERVAL", 0.0)
        self.request_jitter_sec = _env_float("WQMINER_REQUEST_JITTER", 0.0)
        self.metadata_min_interval_sec = _env_float("WQMINER_METADATA_MIN_INTERVAL", 0.0)
        self.metadata_jitter_sec = _env_float("WQMINER_METADATA_JITTER", 0.0)

    @classmethod
    def _throttle(cls, min_interval_sec: float, jitter_sec: float, bucket: str) -> None:
        with cls._global_lock:
            now = time.monotonic()
            if cls._global_last_rate_limit_ts and cls._global_min_interval_floor > 0:
                if now - cls._global_last_rate_limit_ts > 120:
                    cls._global_min_interval_floor = max(0.0, cls._global_min_interval_floor * 0.5)
                    cls._global_last_rate_limit_ts = now
            last = cls._global_last_request_ts if bucket == "global" else cls._global_last_meta_ts
            interval = max(min_interval_sec, cls._global_min_interval_floor)
            wait = interval - (now - last) if interval > 0 else 0.0
            if cls._global_cooldown_until > now:
                wait = max(wait, cls._global_cooldown_until - now)
            if jitter_sec > 0:
                wait += random.uniform(0.0, jitter_sec)
            elif interval > 0 and cls._global_min_interval_floor > 0:
                wait += random.uniform(0.0, min(0.2, interval * 0.2))
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            if bucket == "global":
                cls._global_last_request_ts = now
            else:
                cls._global_last_meta_ts = now

    @classmethod
    def _note_rate_limit(cls, sleep_sec: float) -> None:
        if sleep_sec <= 0:
            return
        now = time.monotonic()
        with cls._global_lock:
            if cls._global_rate_limit_window_start == 0.0 or now - cls._global_rate_limit_window_start > 60:
                cls._global_rate_limit_window_start = now
                cls._global_rate_limit_hits = 0
            cls._global_rate_limit_hits += 1

            until = now + sleep_sec
            if until > cls._global_cooldown_until:
                cls._global_cooldown_until = until

            base = max(1.0, float(sleep_sec))
            mult = 1.0 + min(6.0, cls._global_rate_limit_hits * 0.5)
            max_floor = max(6.0, base)
            floor = min(max_floor, base * mult)
            if floor < base:
                floor = base
            if floor > cls._global_min_interval_floor:
                cls._global_min_interval_floor = floor
            cls._global_last_rate_limit_ts = now

    def authenticate(self, max_retries: int = 5) -> None:
        last_response = None
        for attempt in range(1, max_retries + 1):
            response = self.sess.post(
                f"{self.base_url}/authentication",
                auth=HTTPBasicAuth(self.username, self.password),
                timeout=self.timeout_sec,
            )
            last_response = response

            if response.status_code in (200, 201):
                token = response.headers.get("X-WQB-Session-Token")
                if token:
                    self.sess.headers.update({"X-WQB-Session-Token": token})
                return

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_sec = max(1, int(retry_after))
                else:
                    sleep_sec = min(30, 2 ** (attempt - 1))
                if response.status_code == 429:
                    self._note_rate_limit(sleep_sec)
                logger.warning(
                    "Auth transient failure %s (attempt %s/%s), retrying in %ss",
                    response.status_code,
                    attempt,
                    max_retries,
                    sleep_sec,
                )
                time.sleep(sleep_sec)
                continue

            break

        if last_response is None:
            raise RuntimeError("Authentication failed: no response")
        raise RuntimeError(f"Authentication failed: {last_response.status_code} {last_response.text}")

    def _request(
        self,
        method: str,
        url_or_path: str,
        retry_auth: bool = True,
        max_retries: int = 5,
        **kwargs,
    ) -> requests.Response:
        url = url_or_path if url_or_path.startswith("http") else f"{self.base_url}{url_or_path}"
        last_response = None

        for attempt in range(1, max_retries + 1):
            is_meta = any(token in url for token in ("/data-sets", "/data-fields", "/operators"))
            if is_meta:
                self._throttle(self.metadata_min_interval_sec, self.metadata_jitter_sec, "meta")
            else:
                self._throttle(self.min_request_interval_sec, self.request_jitter_sec, "global")
            response = self.sess.request(method, url, timeout=self.timeout_sec, **kwargs)
            last_response = response

            if response.status_code == 401 and retry_auth and attempt < max_retries:
                logger.warning("401 received on %s %s, re-authenticating and retrying", method, url)
                self.authenticate()
                continue

            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                sleep_sec = self._retry_sleep_seconds(response, attempt)
                if response.status_code == 429:
                    self._note_rate_limit(sleep_sec)
                logger.warning(
                    "Transient status %s on %s %s (attempt %s/%s), retry in %ss",
                    response.status_code,
                    method,
                    url,
                    attempt,
                    max_retries,
                    sleep_sec,
                )
                time.sleep(sleep_sec)
                continue

            return response

        if last_response is None:
            raise RuntimeError(f"Request failed without response: {method} {url}")
        return last_response

    @staticmethod
    def _retry_sleep_seconds(response: requests.Response, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            base = max(1, int(retry_after))
        else:
            base = min(30, 2 ** (attempt - 1))
        jitter = random.uniform(0.0, min(1.0, base * 0.25))
        return base + jitter

    def get_datasets(
        self,
        region: str,
        universe: str,
        delay: int,
        category: str,
        page: int = 1,
        limit: int = 20,
    ) -> List[Dict]:
        params = {
            "category": category,
            "instrumentType": "EQUITY",
            "region": region,
            "universe": universe,
            "delay": delay,
            "page": page,
            "limit": limit,
        }
        response = self._request("GET", "/data-sets", params=params)
        response.raise_for_status()
        return response.json().get("results", [])

    def get_data_fields(
        self,
        dataset_id: str,
        region: str,
        universe: str,
        delay: int,
        page: int,
        limit: int = 50,
    ) -> List[Dict]:
        params = {
            "dataset.id": dataset_id,
            "instrumentType": "EQUITY",
            "region": region,
            "universe": universe,
            "delay": delay,
            "page": page,
            "limit": limit,
        }
        response = self._request("GET", "/data-fields", params=params)
        response.raise_for_status()
        return response.json().get("results", [])

    def get_operators(self, page: int = 1, limit: int = 50) -> List[Dict]:
        params = {
            "page": page,
            "limit": limit,
        }
        response = self._request("GET", "/operators", params=params)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if isinstance(payload.get("results"), list):
                return payload["results"]
            if isinstance(payload.get("items"), list):
                return payload["items"]
        return []

    def fetch_operators(self, max_pages: Optional[int] = None, limit: int = 50) -> List[Dict]:
        all_ops: Dict[str, Dict] = {}
        page = 1
        last_sig = ""
        stagnant_pages = 0

        while True:
            if max_pages is not None and page > max_pages:
                break

            try:
                ops = self.get_operators(page=page, limit=limit)
            except requests.HTTPError:
                break
            if not ops:
                break

            names = sorted(str(op.get("name", "")) for op in ops if op.get("name"))
            sig = "|".join(names)
            if sig and sig == last_sig:
                logger.info("Detected repeated operator page=%s, stopping pagination", page)
                break
            last_sig = sig

            before = len(all_ops)
            for op in ops:
                name = op.get("name")
                if not name:
                    continue
                all_ops[str(name)] = op
            after = len(all_ops)

            if after == before:
                stagnant_pages += 1
            else:
                stagnant_pages = 0

            if stagnant_pages >= 2:
                logger.info("No new operators added after page=%s, stopping", page)
                break
            if max_pages is None and len(ops) < limit:
                break

            page += 1

        return [all_ops[name] for name in sorted(all_ops.keys())]

    def fetch_all_datasets(
        self,
        region: str,
        universe: str,
        delay: int,
        categories: Sequence[str] = ("fundamental", "analyst", "model", "news", "alternative"),
        dataset_page_limit: int = 50,
        dataset_max_pages: Optional[int] = 1,
    ) -> List[Dict]:
        dataset_map: Dict[str, Dict] = {}

        for category in categories:
            page = 1
            last_page_sig = ""
            stagnant_pages = 0
            while True:
                if dataset_max_pages is not None and page > dataset_max_pages:
                    break

                datasets = self.get_datasets(
                    region=region,
                    universe=universe,
                    delay=delay,
                    category=category,
                    page=page,
                    limit=dataset_page_limit,
                )
                if not datasets:
                    break

                ids_on_page = sorted(str(ds.get("id", "")) for ds in datasets if ds.get("id"))
                page_sig = "|".join(ids_on_page)
                if page_sig and page_sig == last_page_sig:
                    logger.info("Detected repeated data-set page for category=%s page=%s, stopping pagination", category, page)
                    break
                last_page_sig = page_sig

                before = len(dataset_map)
                for ds in datasets:
                    ds_id = ds.get("id")
                    if not ds_id:
                        continue
                    dataset_map[ds_id] = ds

                after = len(dataset_map)
                if after == before:
                    stagnant_pages += 1
                else:
                    stagnant_pages = 0

                if stagnant_pages >= 2:
                    logger.info("No new datasets added for category=%s after %s pages, stopping", category, page)
                    break
                if dataset_max_pages is None and len(datasets) < dataset_page_limit:
                    break

                page += 1

        ordered = sorted(dataset_map.values(), key=lambda x: x.get("id", ""))
        return ordered

    def fetch_data_fields_and_datasets(
        self,
        region: str,
        universe: str,
        delay: int,
        categories: Sequence[str] = ("fundamental", "analyst", "model", "news", "alternative"),
        max_datasets: Optional[int] = 10,
        max_pages: Optional[int] = 5,
        dataset_page_limit: int = 50,
        dataset_max_pages: Optional[int] = 1,
    ) -> Tuple[List[DataField], List[Dict]]:
        datasets = self.fetch_all_datasets(
            region=region,
            universe=universe,
            delay=delay,
            categories=categories,
            dataset_page_limit=dataset_page_limit,
            dataset_max_pages=dataset_max_pages,
        )
        if max_datasets is not None:
            datasets = datasets[:max_datasets]

        all_fields: Dict[str, DataField] = {}

        for idx, dataset in enumerate(datasets, start=1):
            dataset_id = dataset.get("id")
            if not dataset_id:
                continue

            logger.info("Fetching fields from dataset %s (%s/%s)", dataset_id, idx, len(datasets))
            page = 1
            last_page_sig = ""
            stagnant_pages = 0
            while True:
                if max_pages is not None and page > max_pages:
                    break

                try:
                    fields = self.get_data_fields(
                        dataset_id=dataset_id,
                        region=region,
                        universe=universe,
                        delay=delay,
                        page=page,
                    )
                except requests.HTTPError:
                    break

                if not fields:
                    break

                ids_on_page = sorted(str(raw.get("id", "")) for raw in fields if raw.get("id"))
                page_sig = "|".join(ids_on_page)
                if page_sig and page_sig == last_page_sig:
                    logger.info("Detected repeated field page for dataset=%s page=%s, stopping pagination", dataset_id, page)
                    break
                last_page_sig = page_sig

                before = len(all_fields)
                for raw in fields:
                    parsed = DataField.from_api(raw)
                    if not parsed.field_id:
                        continue
                    all_fields[parsed.field_id] = parsed

                after = len(all_fields)
                if after == before:
                    stagnant_pages += 1
                else:
                    stagnant_pages = 0

                if stagnant_pages >= 2:
                    logger.info("No new fields added for dataset=%s after page=%s, stopping pagination", dataset_id, page)
                    break
                if max_pages is None and len(fields) < 50:
                    break

                page += 1

        filtered: List[DataField] = []
        for field in all_fields.values():
            if field.region and field.region != region:
                continue
            if field.universe and field.universe != universe:
                continue
            if field.delay and int(field.delay) != int(delay):
                continue
            filtered.append(field)

        return sorted(filtered, key=lambda x: x.field_id), datasets

    def fetch_data_fields(
        self,
        region: str,
        universe: str,
        delay: int,
        categories: Sequence[str] = ("fundamental", "analyst", "model", "news", "alternative"),
        max_datasets: Optional[int] = 10,
        max_pages: Optional[int] = 5,
        dataset_page_limit: int = 50,
        dataset_max_pages: Optional[int] = 1,
    ) -> List[DataField]:
        fields, _ = self.fetch_data_fields_and_datasets(
            region=region,
            universe=universe,
            delay=delay,
            categories=categories,
            max_datasets=max_datasets,
            max_pages=max_pages,
            dataset_page_limit=dataset_page_limit,
            dataset_max_pages=dataset_max_pages,
        )
        return fields

    def load_fallback_default_fields(self) -> List[DataField]:
        file_path = Path(__file__).resolve().parent / "constants" / "default_fields.json"
        with open(file_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return [
            DataField(
                field_id=item.get("id", ""),
                description=item.get("description", ""),
            )
            for item in payload
            if item.get("id")
        ]

    def simulate_expression(
        self,
        expression: str,
        settings: SimulationSettings,
        poll_interval_sec: int = 5,
        max_wait_sec: int = 600,
    ) -> SimulationResult:
        submit = self._request("POST", "/simulations", json=settings.to_api_payload(expression))
        if submit.status_code != 201:
            return SimulationResult(
                expression=expression,
                alpha_id="",
                success=False,
                error_message=f"submit_failed: {submit.status_code} {submit.text}",
            )

        location = submit.headers.get("Location", "")
        if not location:
            return SimulationResult(
                expression=expression,
                alpha_id="",
                success=False,
                error_message="submit_failed: missing Location header",
            )

        progress_url = location if location.startswith("http") else urljoin(f"{self.base_url}/", location.lstrip("/"))
        alpha_id = ""
        deadline = time.time() + max_wait_sec

        while time.time() < deadline:
            progress = self._request("GET", progress_url)
            if progress.status_code != 200:
                time.sleep(self._poll_sleep_seconds(poll_interval_sec))
                continue
            body = progress.json()
            if "alpha" in body and body["alpha"]:
                alpha_id = str(body["alpha"]).rstrip("/").split("/")[-1]
                break
            time.sleep(self._poll_sleep_seconds(poll_interval_sec))

        if not alpha_id:
            return SimulationResult(
                expression=expression,
                alpha_id="",
                success=False,
                error_message="simulation_timeout",
            )

        detail = self._request("GET", f"/alphas/{alpha_id}")
        if detail.status_code != 200:
            return SimulationResult(
                expression=expression,
                alpha_id=alpha_id,
                success=False,
                error_message=f"detail_failed: {detail.status_code} {detail.text}",
            )

        payload = detail.json()
        is_block = payload.get("is", {})
        checks = is_block.get("checks", [])
        passed = 0
        total = len(checks)
        weight_check = ""
        sub_universe_sharpe = 0.0

        for check in checks:
            if check.get("result") == "PASS":
                passed += 1
            if check.get("name") == "CONCENTRATED_WEIGHT":
                weight_check = str(check.get("result", ""))
            if check.get("name") == "LOW_SUB_UNIVERSE_SHARPE":
                sub_universe_sharpe = _to_float(check.get("value"))

        return SimulationResult(
            expression=expression,
            alpha_id=alpha_id,
            success=True,
            sharpe=_to_float(is_block.get("sharpe")),
            fitness=_to_float(is_block.get("fitness")),
            turnover=_to_float(is_block.get("turnover")) * 100.0,
            returns=_to_float(is_block.get("returns")),
            drawdown=_to_float(is_block.get("drawdown")),
            margin=_to_float(is_block.get("margin")),
            passed_checks=passed,
            total_checks=total,
            weight_check=weight_check,
            sub_universe_sharpe=sub_universe_sharpe,
            link=f"{self.PLATFORM_ALPHA_URL}{alpha_id}",
        )

    @staticmethod
    def _poll_sleep_seconds(poll_interval_sec: int) -> float:
        base = max(1.0, float(poll_interval_sec))
        jitter = min(1.0, base * 0.2)
        return base + random.uniform(0.0, jitter)


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
