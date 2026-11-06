#!/usr/bin/env python3
"""Tiny load generator for the event-driven inference service (stdlib only).

It measures the two things that matter for this architecture:

* **accept latency**  - how fast ``POST /inference`` returns ``202`` (should stay
  low no matter how slow the model is, because inference is asynchronous);
* **end-to-end latency / throughput** - time until the consumer(s) store the
  result, which scales with the number of consumer replicas and partitions.

Usage:
    python3 scripts/benchmark.py --requests 60 --concurrency 20
    docker compose up -d --scale kafka_consumer=3   # then compare the results
"""
import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple


def http_json(method: str, url: str, body: Optional[dict] = None, timeout: float = 15) -> Tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, {}


def percentile(values: List[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[index]


def summarize(label: str, values_s: List[float]) -> None:
    if not values_s:
        print(f"{label:<22} n/a")
        return
    ms = [v * 1000 for v in values_s]
    print(
        f"{label:<22} p50={percentile(ms, 50):8.1f} ms  p95={percentile(ms, 95):8.1f} ms  "
        f"max={max(ms):8.1f} ms  mean={statistics.mean(ms):8.1f} ms"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--requests", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=300, help="max seconds to wait for all results")
    args = parser.parse_args()

    def submit(i: int) -> Tuple[Optional[str], float]:
        started = time.perf_counter()
        status, body = http_json(
            "POST", f"{args.base_url}/inference", {"input_data": f"benchmark request {i}", "model_id": "bench"}
        )
        return (body.get("request_id") if status == 202 else None), time.perf_counter() - started

    print(f"Submitting {args.requests} requests with concurrency {args.concurrency} ...")
    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        submitted = list(pool.map(submit, range(args.requests)))
    submit_wall = time.perf_counter() - wall_start

    accepted: Dict[str, float] = {rid: time.perf_counter() for rid, _ in submitted if rid}
    accept_latencies = [latency for rid, latency in submitted if rid]
    rejected = args.requests - len(accepted)

    print("Waiting for the consumers to finish ...")
    finished_at: Dict[str, float] = {}
    statuses: Dict[str, str] = {}
    deadline = time.monotonic() + args.timeout
    pending = set(accepted)
    while pending and time.monotonic() < deadline:
        for rid in list(pending):
            status, body = http_json("GET", f"{args.base_url}/inference/{rid}")
            if status == 200 and body.get("status") in ("COMPLETED", "FAILED"):
                finished_at[rid] = time.perf_counter()
                statuses[rid] = body["status"]
                pending.discard(rid)
        if pending:
            time.sleep(0.5)
    total_wall = time.perf_counter() - wall_start

    e2e = [finished_at[rid] - accepted[rid] for rid in finished_at]
    completed = sum(1 for s in statuses.values() if s == "COMPLETED")

    print("\n=== Results ===")
    print(f"requests sent        : {args.requests} (accepted {len(accepted)}, rejected {rejected})")
    print(f"completed / failed   : {completed} / {len(statuses) - completed}   still pending: {len(pending)}")
    print(f"submit throughput    : {len(accepted) / submit_wall:8.1f} req/s (POST /inference)")
    print(f"end-to-end throughput: {len(finished_at) / total_wall:8.1f} req/s (accepted -> stored)")
    summarize("accept latency", accept_latencies)
    summarize("time after accept", e2e)
    return 0 if not pending and not rejected else 1


if __name__ == "__main__":
    sys.exit(main())
