"""Supabase queue client for producer-side enqueue operations."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

import requests


class SupabaseQueueClient:
    def __init__(
        self,
        *,
        base_url: str,
        service_key: str,
        timeout_sec: int = 20,
        batch_table: str = "alpha_batches",
        job_table: str = "alpha_jobs",
    ) -> None:
        base = str(base_url or "").strip().rstrip("/")
        key = str(service_key or "").strip()
        if not base:
            raise ValueError("base_url is required")
        if not key:
            raise ValueError("service_key is required")
        self.base_url = base
        self.service_key = key
        self.timeout_sec = max(3, int(timeout_sec))
        self.batch_table = str(batch_table or "alpha_batches").strip()
        self.job_table = str(job_table or "alpha_jobs").strip()

    def _headers(self, prefer: str = "return=representation") -> Dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": prefer,
        }

    def _post_rows(self, table: str, rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        items = [dict(x) for x in rows if isinstance(x, dict)]
        if not items:
            return []
        url = f"{self.base_url}/rest/v1/{table}"
        resp = requests.post(
            url,
            headers=self._headers(),
            json=items,
            timeout=self.timeout_sec,
        )
        if resp.status_code >= 300:
            raise RuntimeError(
                f"Supabase insert failed ({resp.status_code}) table={table}: {resp.text}"
            )
        payload = resp.json()
        return payload if isinstance(payload, list) else []

    def create_batch(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        rows = self._post_rows(self.batch_table, [payload])
        if not rows:
            raise RuntimeError("Supabase create_batch returned empty result")
        return dict(rows[0])

    def enqueue_jobs(self, jobs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self._post_rows(self.job_table, jobs)

    def ping(self) -> bool:
        url = f"{self.base_url}/rest/v1/{self.job_table}?select=id&limit=1"
        resp = requests.get(url, headers=self._headers("return=minimal"), timeout=self.timeout_sec)
        return resp.status_code < 400

    def debug_dump(self) -> str:
        payload = {
            "base_url": self.base_url,
            "batch_table": self.batch_table,
            "job_table": self.job_table,
            "timeout_sec": self.timeout_sec,
        }
        return json.dumps(payload, ensure_ascii=False)
