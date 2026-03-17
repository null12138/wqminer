"""WorldQuant Brain API client."""

import asyncio
import json
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth

from .filters import FilterRange
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


def _env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


class WorldQuantBrainClient:
    BASE_URL = "https://api.worldquantbrain.com"
    PLATFORM_ALPHA_URL = "https://platform.worldquantbrain.com/alpha/"
    _global_lock = threading.Lock()
    _auth_lock = threading.RLock()
    _global_last_request_ts = 0.0
    _global_last_meta_ts = 0.0
    _global_cooldown_until = 0.0
    _global_min_interval_floor = 0.0
    _global_last_rate_limit_ts = 0.0
    _global_rate_limit_hits = 0
    _global_rate_limit_window_start = 0.0
    _conn_error_threshold = _env_int("WQMINER_CONN_ERROR_THRESHOLD", 6)
    _conn_error_window_sec = _env_float("WQMINER_CONN_ERROR_WINDOW", 60.0)
    _conn_error_cooldown_sec = _env_float("WQMINER_CONN_ERROR_COOLDOWN", 30.0)
    _auth_error_threshold = _env_int("WQMINER_AUTH_ERROR_THRESHOLD", 4)
    _auth_error_window_sec = _env_float("WQMINER_AUTH_ERROR_WINDOW", 60.0)
    _auth_error_cooldown_sec = _env_float("WQMINER_AUTH_ERROR_COOLDOWN", 12.0)
    _inflight_limit = _env_int("WQMINER_MAX_INFLIGHT", 2)
    _inflight_sem = threading.Semaphore(_inflight_limit) if _inflight_limit > 0 else None
    _force_close = False
    _consecutive_conn_errors = 0
    _last_conn_error_ts = 0.0
    _consecutive_auth_errors = 0
    _last_auth_error_ts = 0.0
    _shared_auth_ts = 0.0
    _shared_headers: Dict[str, str] = {}
    _shared_cookies: Dict[str, str] = {}

    def __init__(
        self,
        username: str,
        password: str,
        timeout_sec: int = 30,
        base_url: Optional[str] = None,
        max_retries: int = 5,
        auto_auth: bool = True,
    ):
        self.username = username
        self.password = password
        self.timeout_sec = timeout_sec
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.max_retries = max(1, int(max_retries))
        self.auto_auth = bool(auto_auth)
        self.sess = requests.Session()
        self.sess.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self.sess.auth = HTTPBasicAuth(username, password)
        if os.getenv("WQMINER_DISABLE_KEEPALIVE", "").strip().lower() in {"1", "true", "yes"}:
            self.sess.headers.update({"Connection": "close"})
        self.min_request_interval_sec = _env_float("WQMINER_MIN_REQUEST_INTERVAL", 0.0)
        self.request_jitter_sec = _env_float("WQMINER_REQUEST_JITTER", 0.0)
        self.metadata_min_interval_sec = _env_float("WQMINER_METADATA_MIN_INTERVAL", 0.0)
        self.metadata_jitter_sec = _env_float("WQMINER_METADATA_JITTER", 0.0)
        self.auth_min_interval_sec = _env_float("WQMINER_AUTH_MIN_INTERVAL", 6.0)
        self._auth_snapshot_ts = 0.0
        self._last_auth_attempt_ts = 0.0
        self._auth_inflight_lock = threading.Lock()

    def _sync_shared_auth(self) -> None:
        cls = self.__class__
        if cls._shared_auth_ts <= self._auth_snapshot_ts:
            return
        with cls._auth_lock:
            if cls._shared_auth_ts <= self._auth_snapshot_ts:
                return
            if cls._shared_headers:
                self.sess.headers.update(cls._shared_headers)
            if cls._shared_cookies:
                self.sess.cookies.update(cls._shared_cookies)
            self._auth_snapshot_ts = cls._shared_auth_ts

    def _recent_shared_auth(self) -> bool:
        cls = self.__class__
        if cls._shared_auth_ts <= 0:
            return False
        return (time.monotonic() - cls._shared_auth_ts) < self.auth_min_interval_sec

    def _has_auth_token(self) -> bool:
        if self.sess.headers.get("X-WQB-Session-Token"):
            return True
        shared = self.__class__._shared_headers.get("X-WQB-Session-Token")
        return bool(shared)

    def _ensure_authenticated(self) -> None:
        if not self.auto_auth:
            return
        if self._recent_shared_auth():
            self._sync_shared_auth()
            return
        if self._has_auth_token():
            return
        with self._auth_inflight_lock:
            if self._recent_shared_auth():
                self._sync_shared_auth()
                return
            if self._has_auth_token():
                return
            self.authenticate()

    @classmethod
    def _note_connect_failure(cls, sleep_sec: float) -> None:
        if sleep_sec <= 0:
            return
        now = time.monotonic()
        with cls._global_lock:
            if cls._last_conn_error_ts == 0.0 or now - cls._last_conn_error_ts > cls._conn_error_window_sec:
                cls._consecutive_conn_errors = 0
            cls._consecutive_conn_errors += 1
            cls._last_conn_error_ts = now

            cooldown = now + min(8.0, sleep_sec)
            if cls._consecutive_conn_errors >= cls._conn_error_threshold:
                extra = min(cls._conn_error_cooldown_sec, cls._consecutive_conn_errors * 2.0)
                cooldown = max(cooldown, now + extra)
                floor = min(10.0, max(1.0, extra * 0.5))
                if floor > cls._global_min_interval_floor:
                    cls._global_min_interval_floor = floor
                cls._force_close = True
            if cooldown > cls._global_cooldown_until:
                cls._global_cooldown_until = cooldown
            floor = min(4.0, max(0.5, sleep_sec * 0.5))
            if floor > cls._global_min_interval_floor:
                cls._global_min_interval_floor = floor

    @classmethod
    def _note_auth_error(cls) -> None:
        now = time.monotonic()
        with cls._global_lock:
            if cls._last_auth_error_ts == 0.0 or now - cls._last_auth_error_ts > cls._auth_error_window_sec:
                cls._consecutive_auth_errors = 0
            cls._consecutive_auth_errors += 1
            cls._last_auth_error_ts = now
            if cls._consecutive_auth_errors >= cls._auth_error_threshold:
                cooldown = now + cls._auth_error_cooldown_sec
                if cooldown > cls._global_cooldown_until:
                    cls._global_cooldown_until = cooldown

    @classmethod
    def _reset_error_counters(cls, conn: bool = True, auth: bool = True) -> None:
        with cls._global_lock:
            if conn:
                cls._consecutive_conn_errors = 0
                cls._last_conn_error_ts = 0.0
                cls._force_close = False
            if auth:
                cls._consecutive_auth_errors = 0
                cls._last_auth_error_ts = 0.0

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
        if max_retries is None:
            max_retries = self.max_retries
        cls = self.__class__
        with cls._auth_lock:
            if cls._shared_auth_ts and cls._shared_auth_ts > self._auth_snapshot_ts:
                self._sync_shared_auth()
                if self._recent_shared_auth():
                    return

        last_response = None
        for attempt in range(1, max_retries + 1):
            with cls._auth_lock:
                if cls._shared_auth_ts and cls._shared_auth_ts > self._auth_snapshot_ts:
                    self._sync_shared_auth()
                    if self._recent_shared_auth():
                        return
                now = time.monotonic()
                if self._last_auth_attempt_ts and now - self._last_auth_attempt_ts < self.auth_min_interval_sec:
                    sleep_sec = max(0.5, self.auth_min_interval_sec - (now - self._last_auth_attempt_ts))
                else:
                    sleep_sec = 0.0
                if sleep_sec > 0:
                    logger.info("Auth throttled, waiting %.2fs before retry", sleep_sec)
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            self._last_auth_attempt_ts = time.monotonic()
            try:
                response = self.sess.post(
                    f"{self.base_url}/authentication",
                    auth=HTTPBasicAuth(self.username, self.password),
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as exc:
                if attempt < max_retries:
                    sleep_sec = min(30, 2 ** (attempt - 1))
                    self._reset_session()
                    logger.warning("Auth request error (%s), retrying in %ss", exc, sleep_sec)
                    time.sleep(sleep_sec)
                    continue
                raise RuntimeError(f"Authentication failed: {exc}") from exc
            last_response = response

            if response.status_code in (200, 201):
                token = response.headers.get("X-WQB-Session-Token")
                if token:
                    self.sess.headers.update({"X-WQB-Session-Token": token})
                with cls._auth_lock:
                    cls._shared_headers = {"X-WQB-Session-Token": token} if token else {}
                    cls._shared_cookies = requests.utils.dict_from_cookiejar(self.sess.cookies)
                    cls._shared_auth_ts = time.monotonic()
                    self._auth_snapshot_ts = cls._shared_auth_ts
                cls._reset_error_counters(conn=False, auth=True)
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
        max_retries: Optional[int] = None,
        **kwargs,
    ) -> requests.Response:
        if max_retries is None:
            max_retries = self.max_retries
        url = url_or_path if url_or_path.startswith("http") else f"{self.base_url}{url_or_path}"
        last_response = None

        for attempt in range(1, max_retries + 1):
            self._ensure_authenticated()
            self._sync_shared_auth()
            if self.__class__._force_close and self.sess.headers.get("Connection") != "close":
                self.sess.headers.update({"Connection": "close"})
            is_meta = any(token in url for token in ("/data-sets", "/data-fields", "/operators"))
            if is_meta:
                self._throttle(self.metadata_min_interval_sec, self.metadata_jitter_sec, "meta")
            else:
                self._throttle(self.min_request_interval_sec, self.request_jitter_sec, "global")
            try:
                sem = self.__class__._inflight_sem
                if sem is None:
                    response = self.sess.request(method, url, timeout=self.timeout_sec, **kwargs)
                else:
                    with sem:
                        response = self.sess.request(method, url, timeout=self.timeout_sec, **kwargs)
            except requests.RequestException as exc:
                if attempt < max_retries:
                    sleep_sec = min(30, 2 ** (attempt - 1))
                    self._reset_session()
                    self._note_connect_failure(sleep_sec)
                    logger.warning("Request error on %s %s (%s), retry in %ss", method, url, exc, sleep_sec)
                    time.sleep(sleep_sec)
                    continue
                raise RuntimeError(f"Request failed: {method} {url} {exc}") from exc
            last_response = response
            if response.status_code < 400:
                self._reset_error_counters(conn=True, auth=True)

            if response.status_code == 401 and retry_auth and attempt < max_retries:
                self._note_auth_error()
                logger.warning("401 received on %s %s, re-authenticating and retrying", method, url)
                if self._recent_shared_auth():
                    sleep_sec = max(0.5, self.auth_min_interval_sec * 0.5)
                    time.sleep(sleep_sec)
                    self._sync_shared_auth()
                else:
                    self._reset_session()
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

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> Optional[float]:
        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            return None
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _build_query_url(path: str, params: Sequence[str]) -> str:
        if not params:
            return path
        query = "&".join(params)
        return f"{path}?{query}".replace("+", "%2B")

    # Adapted from wqb (MIT License, Copyright (c) 2025 Rocky Haotian Du).
    def _retry_with_retry_after(
        self,
        method: str,
        url_or_path: str,
        *,
        max_tries: int | Iterable[int] = 600,
        max_key_errors: int = 1,
        max_value_errors: int = 1,
        delay_key_error: float = 2.0,
        delay_value_error: float = 2.0,
        **kwargs,
    ) -> requests.Response:
        if isinstance(max_tries, int):
            if max_tries <= 0:
                raise ValueError("max_tries must be positive")
            tries_iter: Iterable[int] = range(max_tries)
        else:
            tries_iter = max_tries

        resp: Optional[requests.Response] = None
        key_errors = 0
        value_errors = 0
        for _ in tries_iter:
            resp = self._request(method, url_or_path, **kwargs)
            retry_after = resp.headers.get("Retry-After")
            if retry_after is None:
                key_errors += 1
                if key_errors >= max(1, int(max_key_errors)):
                    break
                time.sleep(max(0.0, float(delay_key_error)))
                continue
            try:
                sleep_sec = float(retry_after)
            except (TypeError, ValueError):
                value_errors += 1
                if value_errors >= max(1, int(max_value_errors)):
                    break
                time.sleep(max(0.0, float(delay_value_error)))
                continue
            if sleep_sec > 0:
                time.sleep(sleep_sec)

        if resp is None:
            raise RuntimeError("Retry loop ended without a response")
        return resp

    def _reset_session(self) -> None:
        headers = dict(self.sess.headers)
        auth = self.sess.auth
        try:
            self.sess.close()
        except Exception:
            pass
        self.sess = requests.Session()
        self.sess.headers.update(headers)
        self.sess.auth = auth
        self._sync_shared_auth()

    def _clone_client(self) -> "WorldQuantBrainClient":
        client = self.__class__(
            username=self.username,
            password=self.password,
            timeout_sec=self.timeout_sec,
            base_url=self.base_url,
            max_retries=self.max_retries,
            auto_auth=self.auto_auth,
        )
        client._sync_shared_auth()
        return client

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

    def get_dataset(self, dataset_id: str) -> Dict:
        response = self._request("GET", f"/data-sets/{dataset_id}")
        response.raise_for_status()
        return response.json()

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

    def get_data_field(self, field_id: str) -> Dict:
        response = self._request("GET", f"/data-fields/{field_id}")
        response.raise_for_status()
        return response.json()

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

    def search_datasets_limited(
        self,
        region: str,
        delay: int,
        universe: str,
        *,
        instrument_type: str = "EQUITY",
        search: Optional[str] = None,
        category: Optional[str] = None,
        theme: Optional[bool] = None,
        coverage: Optional[FilterRange] = None,
        value_score: Optional[FilterRange] = None,
        alpha_count: Optional[FilterRange] = None,
        user_count: Optional[FilterRange] = None,
        order: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        others: Optional[Iterable[str]] = None,
    ) -> Dict:
        limit = min(max(int(limit), 1), 50)
        offset = min(max(int(offset), 0), 10000 - limit)
        params = [
            f"region={region}",
            f"delay={delay}",
            f"universe={universe}",
            f"instrumentType={instrument_type}",
        ]
        if search is not None:
            params.append(f"search={search}")
        if category is not None:
            params.append(f"category={category}")
        if theme is not None:
            params.append(f"theme={'true' if theme else 'false'}")
        if coverage is not None:
            params.append(coverage.to_params("coverage"))
        if value_score is not None:
            params.append(value_score.to_params("valueScore"))
        if alpha_count is not None:
            params.append(alpha_count.to_params("alphaCount"))
        if user_count is not None:
            params.append(user_count.to_params("userCount"))
        if order is not None:
            params.append(f"order={order}")
        params.append(f"limit={limit}")
        params.append(f"offset={offset}")
        if others is not None:
            params.extend(list(others))
        url = self._build_query_url("/data-sets", params)
        response = self._request("GET", url)
        response.raise_for_status()
        return response.json()

    def search_datasets(
        self,
        region: str,
        delay: int,
        universe: str,
        *,
        limit: int = 50,
        offset: int = 0,
        max_pages: Optional[int] = None,
        **kwargs,
    ) -> List[Dict]:
        results: List[Dict] = []
        page = 0
        total = None
        while True:
            payload = self.search_datasets_limited(
                region,
                delay,
                universe,
                limit=limit,
                offset=offset,
                **kwargs,
            )
            items = payload.get("results") or payload.get("items") or []
            results.extend(items)
            total = payload.get("count", total)
            if not items or len(items) < min(max(int(limit), 1), 50):
                break
            offset += int(limit)
            page += 1
            if max_pages is not None and page >= int(max_pages):
                break
            if total is not None and offset >= int(total):
                break
        return results

    def search_fields_limited(
        self,
        region: str,
        delay: int,
        universe: str,
        *,
        instrument_type: str = "EQUITY",
        dataset_id: Optional[str] = None,
        search: Optional[str] = None,
        category: Optional[str] = None,
        theme: Optional[bool] = None,
        coverage: Optional[FilterRange] = None,
        field_type: Optional[str] = None,
        alpha_count: Optional[FilterRange] = None,
        user_count: Optional[FilterRange] = None,
        order: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
        others: Optional[Iterable[str]] = None,
    ) -> Dict:
        limit = min(max(int(limit), 1), 50)
        offset = min(max(int(offset), 0), 10000 - limit)
        params = [
            f"region={region}",
            f"delay={delay}",
            f"universe={universe}",
            f"instrumentType={instrument_type}",
        ]
        if dataset_id is not None:
            params.append(f"dataset.id={dataset_id}")
        if search is not None:
            params.append(f"search={search}")
        if category is not None:
            params.append(f"category={category}")
        if theme is not None:
            params.append(f"theme={'true' if theme else 'false'}")
        if coverage is not None:
            params.append(coverage.to_params("coverage"))
        if field_type is not None:
            params.append(f"type={field_type}")
        if alpha_count is not None:
            params.append(alpha_count.to_params("alphaCount"))
        if user_count is not None:
            params.append(user_count.to_params("userCount"))
        if order is not None:
            params.append(f"order={order}")
        params.append(f"limit={limit}")
        params.append(f"offset={offset}")
        if others is not None:
            params.extend(list(others))
        url = self._build_query_url("/data-fields", params)
        response = self._request("GET", url)
        response.raise_for_status()
        return response.json()

    def search_fields(
        self,
        region: str,
        delay: int,
        universe: str,
        *,
        limit: int = 50,
        offset: int = 0,
        max_pages: Optional[int] = None,
        **kwargs,
    ) -> List[Dict]:
        results: List[Dict] = []
        page = 0
        total = None
        while True:
            payload = self.search_fields_limited(
                region,
                delay,
                universe,
                limit=limit,
                offset=offset,
                **kwargs,
            )
            items = payload.get("results") or payload.get("items") or []
            results.extend(items)
            total = payload.get("count", total)
            if not items or len(items) < min(max(int(limit), 1), 50):
                break
            offset += int(limit)
            page += 1
            if max_pages is not None and page >= int(max_pages):
                break
            if total is not None and offset >= int(total):
                break
        return results

    def get_alpha(self, alpha_id: str) -> Dict:
        response = self._request("GET", f"/alphas/{alpha_id}")
        response.raise_for_status()
        return response.json()

    def filter_alphas_limited(
        self,
        *,
        name: Optional[str] = None,
        competition: Optional[bool] = None,
        alpha_type: Optional[str] = None,
        language: Optional[str] = None,
        date_created: Optional[FilterRange] = None,
        favorite: Optional[bool] = None,
        date_submitted: Optional[FilterRange] = None,
        start_date: Optional[FilterRange] = None,
        status: Optional[str] = None,
        category: Optional[str] = None,
        color: Optional[str] = None,
        tag: Optional[str] = None,
        hidden: Optional[bool] = None,
        region: Optional[str] = None,
        instrument_type: Optional[str] = None,
        universe: Optional[str] = None,
        delay: Optional[int] = None,
        decay: Optional[FilterRange] = None,
        neutralization: Optional[str] = None,
        truncation: Optional[FilterRange] = None,
        unit_handling: Optional[str] = None,
        nan_handling: Optional[str] = None,
        pasteurization: Optional[str] = None,
        sharpe: Optional[FilterRange] = None,
        returns: Optional[FilterRange] = None,
        pnl: Optional[FilterRange] = None,
        turnover: Optional[FilterRange] = None,
        drawdown: Optional[FilterRange] = None,
        margin: Optional[FilterRange] = None,
        fitness: Optional[FilterRange] = None,
        book_size: Optional[FilterRange] = None,
        long_count: Optional[FilterRange] = None,
        short_count: Optional[FilterRange] = None,
        sharpe60: Optional[FilterRange] = None,
        sharpe125: Optional[FilterRange] = None,
        sharpe250: Optional[FilterRange] = None,
        sharpe500: Optional[FilterRange] = None,
        os_is_sharpe_ratio: Optional[FilterRange] = None,
        pre_close_sharpe: Optional[FilterRange] = None,
        pre_close_sharpe_ratio: Optional[FilterRange] = None,
        self_correlation: Optional[FilterRange] = None,
        prod_correlation: Optional[FilterRange] = None,
        order: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        others: Optional[Iterable[str]] = None,
    ) -> Dict:
        limit = min(max(int(limit), 1), 100)
        offset = min(max(int(offset), 0), 10000 - limit)
        params: List[str] = []
        if name:
            prefix = name[:1]
            params.append(f"name{name if prefix in {'~', '='} else '~' + name}")
        if competition is not None:
            params.append(f"competition={'true' if competition else 'false'}")
        if alpha_type is not None:
            params.append(f"type={alpha_type}")
        if language is not None:
            params.append(f"settings.language={language}")
        if date_created is not None:
            params.append(date_created.to_params("dateCreated"))
        if favorite is not None:
            params.append(f"favorite={'true' if favorite else 'false'}")
        if date_submitted is not None:
            params.append(date_submitted.to_params("dateSubmitted"))
        if start_date is not None:
            params.append(start_date.to_params("os.startDate"))
        if status is not None:
            params.append(f"status={status}")
        if category is not None:
            params.append(f"category={category}")
        if color is not None:
            params.append(f"color={color}")
        if tag is not None:
            params.append(f"tag={tag}")
        if hidden is not None:
            params.append(f"hidden={'true' if hidden else 'false'}")
        if region is not None:
            params.append(f"settings.region={region}")
        if instrument_type is not None:
            params.append(f"settings.instrumentType={instrument_type}")
        if universe is not None:
            params.append(f"settings.universe={universe}")
        if delay is not None:
            params.append(f"settings.delay={delay}")
        if decay is not None:
            params.append(decay.to_params("settings.decay"))
        if neutralization is not None:
            params.append(f"settings.neutralization={neutralization}")
        if truncation is not None:
            params.append(truncation.to_params("settings.truncation"))
        if unit_handling is not None:
            params.append(f"settings.unitHandling={unit_handling}")
        if nan_handling is not None:
            params.append(f"settings.nanHandling={nan_handling}")
        if pasteurization is not None:
            params.append(f"settings.pasteurization={pasteurization}")
        if sharpe is not None:
            params.append(sharpe.to_params("is.sharpe"))
        if returns is not None:
            params.append(returns.to_params("is.returns"))
        if pnl is not None:
            params.append(pnl.to_params("is.pnl"))
        if turnover is not None:
            params.append(turnover.to_params("is.turnover"))
        if drawdown is not None:
            params.append(drawdown.to_params("is.drawdown"))
        if margin is not None:
            params.append(margin.to_params("is.margin"))
        if fitness is not None:
            params.append(fitness.to_params("is.fitness"))
        if book_size is not None:
            params.append(book_size.to_params("is.bookSize"))
        if long_count is not None:
            params.append(long_count.to_params("is.longCount"))
        if short_count is not None:
            params.append(short_count.to_params("is.shortCount"))
        if sharpe60 is not None:
            params.append(sharpe60.to_params("os.sharpe60"))
        if sharpe125 is not None:
            params.append(sharpe125.to_params("os.sharpe125"))
        if sharpe250 is not None:
            params.append(sharpe250.to_params("os.sharpe250"))
        if sharpe500 is not None:
            params.append(sharpe500.to_params("os.sharpe500"))
        if os_is_sharpe_ratio is not None:
            params.append(os_is_sharpe_ratio.to_params("os.osISSharpeRatio"))
        if pre_close_sharpe is not None:
            params.append(pre_close_sharpe.to_params("os.preCloseSharpe"))
        if pre_close_sharpe_ratio is not None:
            params.append(pre_close_sharpe_ratio.to_params("os.preCloseSharpeRatio"))
        if self_correlation is not None:
            params.append(self_correlation.to_params("is.selfCorrelation"))
        if prod_correlation is not None:
            params.append(prod_correlation.to_params("is.prodCorrelation"))
        if order is not None:
            params.append(f"order={order}")
        params.append(f"limit={limit}")
        params.append(f"offset={offset}")
        if others is not None:
            params.extend(list(others))
        url = self._build_query_url("/users/self/alphas", params)
        response = self._request("GET", url)
        response.raise_for_status()
        return response.json()

    def filter_alphas(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        max_pages: Optional[int] = None,
        **kwargs,
    ) -> List[Dict]:
        results: List[Dict] = []
        page = 0
        total = None
        while True:
            payload = self.filter_alphas_limited(limit=limit, offset=offset, **kwargs)
            items = payload.get("results") or payload.get("items") or payload.get("alphas") or []
            results.extend(items)
            total = payload.get("count", total)
            if not items or len(items) < min(max(int(limit), 1), 100):
                break
            offset += int(limit)
            page += 1
            if max_pages is not None and page >= int(max_pages):
                break
            if total is not None and offset >= int(total):
                break
        return results

    def patch_alpha_properties(
        self,
        alpha_id: str,
        *,
        favorite: Optional[bool] = None,
        hidden: Optional[bool] = None,
        name: Optional[str] = None,
        category: Optional[str] = None,
        tags: Optional[Sequence[str] | str] = None,
        color: Optional[str] = None,
        regular_description: Optional[str] = None,
        clear_name: bool = False,
        clear_category: bool = False,
        clear_tags: bool = False,
        clear_color: bool = False,
        clear_regular_description: bool = False,
    ) -> Dict:
        url = f"/alphas/{alpha_id}"
        props: Dict[str, Any] = {}
        if favorite is not None:
            props["favorite"] = favorite
        if hidden is not None:
            props["hidden"] = hidden
        if clear_name:
            props["name"] = None
        elif name is not None:
            props["name"] = name
        if clear_category:
            props["category"] = None
        elif category is not None:
            props["category"] = category
        if clear_tags:
            props["tags"] = []
        elif tags is not None:
            props["tags"] = [tags] if isinstance(tags, str) else list(tags)
        if clear_color:
            props["color"] = None
        elif color is not None:
            props["color"] = color
        if clear_regular_description:
            props.setdefault("regular", {})["description"] = None
        elif regular_description is not None:
            props.setdefault("regular", {})["description"] = regular_description
        response = self._request("PATCH", url, json=props)
        response.raise_for_status()
        return response.json()

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
                retry_after = self._parse_retry_after(progress)
                time.sleep(retry_after if retry_after is not None else self._poll_sleep_seconds(poll_interval_sec))
                continue
            body = progress.json()
            if "alpha" in body and body["alpha"]:
                alpha_id = str(body["alpha"]).rstrip("/").split("/")[-1]
                break
            retry_after = self._parse_retry_after(progress)
            time.sleep(retry_after if retry_after is not None else self._poll_sleep_seconds(poll_interval_sec))

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

    def check_alpha(
        self,
        alpha_id: str,
        *,
        max_tries: int | Iterable[int] = 600,
    ) -> requests.Response:
        return self._retry_with_retry_after("GET", f"/alphas/{alpha_id}/check", max_tries=max_tries)

    def submit_alpha(
        self,
        alpha_id: str,
        *,
        max_tries: int | Iterable[int] = 600,
        allow_http_fallback: bool = True,
    ) -> requests.Response:
        resp = self._retry_with_retry_after("POST", f"/alphas/{alpha_id}/submit", max_tries=max_tries)
        if not allow_http_fallback:
            return resp
        if resp.status_code in (404, 405) and "api.worldquantbrain.com" in self.base_url:
            fallback_url = f"http://api.worldquantbrain.com:443/alphas/{alpha_id}/submit"
            return self._retry_with_retry_after("POST", fallback_url, max_tries=max_tries)
        return resp

    async def async_simulate_expression(
        self,
        expression: str,
        settings: SimulationSettings,
        poll_interval_sec: int = 5,
        max_wait_sec: int = 600,
    ) -> SimulationResult:
        return await asyncio.to_thread(
            self.simulate_expression,
            expression,
            settings,
            poll_interval_sec,
            max_wait_sec,
        )

    async def async_check_alpha(self, alpha_id: str, *, max_tries: int | Iterable[int] = 600) -> requests.Response:
        return await asyncio.to_thread(self.check_alpha, alpha_id, max_tries=max_tries)

    async def async_submit_alpha(
        self,
        alpha_id: str,
        *,
        max_tries: int | Iterable[int] = 600,
        allow_http_fallback: bool = True,
    ) -> requests.Response:
        return await asyncio.to_thread(
            self.submit_alpha,
            alpha_id,
            max_tries=max_tries,
            allow_http_fallback=allow_http_fallback,
        )

    async def concurrent_simulate_expressions(
        self,
        expressions: Sequence[str],
        settings: SimulationSettings,
        *,
        concurrency: int = 4,
        return_exceptions: bool = False,
        poll_interval_sec: int = 5,
        max_wait_sec: int = 600,
    ) -> List[SimulationResult | BaseException]:
        self._ensure_authenticated()
        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def run_one(expr: str) -> SimulationResult:
            async with sem:
                return await asyncio.to_thread(
                    self._clone_client().simulate_expression,
                    expr,
                    settings,
                    poll_interval_sec,
                    max_wait_sec,
                )

        tasks = [asyncio.create_task(run_one(expr)) for expr in expressions]
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)

    async def concurrent_check_alphas(
        self,
        alpha_ids: Sequence[str],
        *,
        concurrency: int = 6,
        return_exceptions: bool = False,
        max_tries: int | Iterable[int] = 600,
    ) -> List[requests.Response | BaseException]:
        self._ensure_authenticated()
        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def run_one(alpha_id: str) -> requests.Response:
            async with sem:
                return await asyncio.to_thread(self._clone_client().check_alpha, alpha_id, max_tries=max_tries)

        tasks = [asyncio.create_task(run_one(alpha_id)) for alpha_id in alpha_ids]
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)

    async def concurrent_submit_alphas(
        self,
        alpha_ids: Sequence[str],
        *,
        concurrency: int = 6,
        return_exceptions: bool = False,
        max_tries: int | Iterable[int] = 600,
        allow_http_fallback: bool = True,
    ) -> List[requests.Response | BaseException]:
        self._ensure_authenticated()
        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def run_one(alpha_id: str) -> requests.Response:
            async with sem:
                return await asyncio.to_thread(
                    self._clone_client().submit_alpha,
                    alpha_id,
                    max_tries=max_tries,
                    allow_http_fallback=allow_http_fallback,
                )

        tasks = [asyncio.create_task(run_one(alpha_id)) for alpha_id in alpha_ids]
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)

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
