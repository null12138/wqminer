#!/usr/bin/env python3
"""Run continuous optimization rounds by repeatedly invoking run_round.py."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import List


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run continuous OpenClaw optimization rounds")
    parser.add_argument("--workspace", default=".", help="Project root")
    parser.add_argument("--run-round-script", default="", help="Path to run_round.py")
    parser.add_argument("--max-rounds", type=int, default=12, help="Maximum rounds to run")
    parser.add_argument("--sleep-seconds", type=int, default=20, help="Sleep between rounds")
    parser.add_argument("--round-prefix", default="openclaw_loop", help="Round tag prefix")
    parser.add_argument(
        "--keep-running-after-hit",
        action="store_true",
        help="Do not stop when bar hit is found",
    )
    parser.add_argument(
        "round_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to run_round.py; place after '--'",
    )
    args = parser.parse_args()
    if args.round_args and args.round_args[0] == "--":
        args.round_args = args.round_args[1:]
    return args


def value_after(args: List[str], flag: str, default: str) -> str:
    if flag in args:
        idx = args.index(flag)
        if idx + 1 < len(args):
            return args[idx + 1]
    return default


def stream_command(cmd: List[str], cwd: Path) -> int:
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
    proc.wait()
    return int(proc.returncode or 0)


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()

    run_round_script = Path(args.run_round_script).resolve() if args.run_round_script else (Path(__file__).resolve().parent / "run_round.py")
    if not run_round_script.exists():
        raise FileNotFoundError(f"run_round.py not found: {run_round_script}")

    output_dir_value = value_after(args.round_args, "--output-dir", "results/submit_single")
    output_dir = (workspace / output_dir_value).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    loop_started = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    loop_results = {
        "started_at": loop_started,
        "workspace": str(workspace),
        "rounds": [],
    }

    print(f"[loop] workspace={workspace}")
    print(f"[loop] run_round_script={run_round_script}")
    print(f"[loop] max_rounds={args.max_rounds}")

    for round_idx in range(1, max(1, args.max_rounds) + 1):
        round_tag = f"{args.round_prefix}_{round_idx:02d}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
        cmd = [
            sys.executable,
            str(run_round_script),
            "--workspace",
            str(workspace),
            "--round-tag",
            round_tag,
            *args.round_args,
        ]

        print(f"\n[loop] starting round {round_idx}/{args.max_rounds}: {round_tag}")
        code = stream_command(cmd, cwd=workspace)

        report_path = output_dir / f"openclaw_round_{round_tag}_report.json"
        round_info = {
            "round_idx": round_idx,
            "round_tag": round_tag,
            "exit_code": code,
            "report_path": str(report_path),
            "bar_hit_count": 0,
        }

        if report_path.exists():
            try:
                payload = json.loads(report_path.read_text(encoding="utf-8"))
                bar = payload.get("bar", {})
                round_info["bar_hit_count"] = int(bar.get("bar_hit_count", 0))
                round_info["max_sharpe"] = float(bar.get("max_sharpe", 0.0))
                round_info["max_fitness"] = float(bar.get("max_fitness", 0.0))
                round_info["avg_sharpe"] = float(bar.get("avg_sharpe", 0.0))
                round_info["avg_fitness"] = float(bar.get("avg_fitness", 0.0))
                round_info["avg_turnover"] = float(bar.get("avg_turnover", 0.0))
            except Exception:  # noqa: BLE001
                pass

        loop_results["rounds"].append(round_info)

        print(
            f"[loop] round_done tag={round_tag} exit={code} "
            f"bar_hits={round_info.get('bar_hit_count', 0)}"
        )

        if code != 0:
            print("[loop] stopping because round command failed")
            break

        if not args.keep_running_after_hit and int(round_info.get("bar_hit_count", 0)) > 0:
            print("[loop] stopping because bar hit found")
            break

        if round_idx < args.max_rounds:
            time.sleep(max(0, args.sleep_seconds))

    finished = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    loop_results["finished_at"] = finished

    loop_report_path = output_dir / f"openclaw_loop_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    loop_report_path.write_text(json.dumps(loop_results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[loop] report={loop_report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
