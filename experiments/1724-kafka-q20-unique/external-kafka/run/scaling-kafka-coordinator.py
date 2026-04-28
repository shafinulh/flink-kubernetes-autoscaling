#!/usr/bin/env python3
"""
Coordinate an external-host Kafka producer around Flink autoscaling events.

When the Flink consumer job rescales (detected via CREATED timestamp change
in the Flink REST API — the adaptive scheduler transitions RUNNING -> CREATED
-> RUNNING in milliseconds, too fast to catch by polling status), this script:
  1. Pauses the standalone Kafka producer on the target host
  2. Stops the producer and resets Kafka topics (wipe all data)
  3. Waits for the consumer job to return to RUNNING
  4. Restarts the producer from Nexmark event id 1 for the full run

This ensures the consumer (which restarts from offset 0 with no checkpoint)
always sees an empty topic and reprocesses the full Nexmark run from scratch.

Usage:
    ./scaling-kafka-coordinator.py --tps 50000 --events 300000000
    ./scaling-kafka-coordinator.py --tps 50000 --events 300000000 --parallelism 1
    ./scaling-kafka-coordinator.py --tps 100000 --events 125000000 --parallelism 4 --max-emit-speed false
    ./scaling-kafka-coordinator.py --producer-rest-port 18081
    ./scaling-kafka-coordinator.py --deployment flink --flink-port 8081

Environment:
    TARGET_HOST=c153
    TARGET_IP=142.150.234.153
    HOST_TAG=c153
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../../.."))
CLUSTER_ROOT = os.path.join(REPO_ROOT, "cluster")
CLUSTER_ENV = os.path.join(CLUSTER_ROOT, "config", "env.sh")
TARGET_HOST = os.environ.get("TARGET_HOST", "c153")
TARGET_IP = os.environ.get("TARGET_IP", "")
HOST_TAG = os.environ.get("HOST_TAG", TARGET_HOST)
KAFKA_CONTAINER = os.environ.get("KAFKA_CONTAINER", f"external-kafka-{HOST_TAG}")
PRODUCER_CONTAINER = os.environ.get("PRODUCER_CONTAINER", f"standalone-insert-kafka-{HOST_TAG}")
KAFKA_TOPICS = ["nexmark-person", "nexmark-auction", "nexmark-bid"]


def script_env() -> dict[str, str]:
    if not TARGET_HOST:
        raise RuntimeError("TARGET_HOST is required")
    env = os.environ.copy()
    env.setdefault("TARGET_HOST", TARGET_HOST)
    env.setdefault("HOST_TAG", HOST_TAG)
    if TARGET_IP:
        env.setdefault("TARGET_IP", TARGET_IP)
    return env


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def ssh_remote(cmd: str) -> str:
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", TARGET_HOST, cmd],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"SSH command failed: {cmd}\nstderr: {result.stderr.strip()}")
    return result.stdout.strip()


def flink_rest_get(port: int, path: str) -> dict:
    result = subprocess.run(
        ["curl", "-sf", f"http://localhost:{port}{path}"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Flink REST GET {path} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def flink_rest_healthy(port: int) -> bool:
    try:
        flink_rest_get(port, "/jobs/overview")
        return True
    except Exception:
        return False


def is_active_job_status(status: str | None) -> bool:
    return status in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING", "RECONCILING")


def get_active_job(port: int, job_name: str | None = None) -> dict | None:
    data = flink_rest_get(port, "/jobs/overview")
    candidates = []
    for job in data.get("jobs", []):
        if not is_active_job_status(job.get("state")):
            continue
        if job_name and job.get("name") != job_name:
            continue
        candidates.append(job)

    if not candidates:
        return None

    candidates.sort(
        key=lambda j: (
            j.get("start-time", -1),
            j.get("last-modification", -1),
        ),
        reverse=True,
    )
    return candidates[0]


def get_job_status(port: int, job_id: str) -> str:
    data = flink_rest_get(port, f"/jobs/{job_id}")
    return data.get("state", "UNKNOWN")


def get_job_detail(port: int, job_id: str) -> dict:
    """Return the full job detail including timestamps."""
    return flink_rest_get(port, f"/jobs/{job_id}")


def get_created_timestamp(job_detail: dict) -> int:
    """Extract the CREATED timestamp (ms since epoch) from job detail.
    This timestamp is updated every time the adaptive scheduler rescales."""
    return job_detail.get("timestamps", {}).get("CREATED", 0)


def pause_producer() -> None:
    log(f"Pausing producer container on {TARGET_HOST}...")
    try:
        ssh_remote(f"sudo -n docker pause {PRODUCER_CONTAINER}")
        log("Producer paused.")
    except RuntimeError as e:
        log(f"Warning: could not pause producer (may already be stopped): {e}")


def stop_producer() -> None:
    log(f"Stopping producer container on {TARGET_HOST}...")
    try:
        ssh_remote(f"sudo -n docker rm -f {PRODUCER_CONTAINER}")
        log("Producer stopped.")
    except RuntimeError as e:
        log(f"Warning: could not stop producer: {e}")


def reset_kafka() -> None:
    log(f"Resetting Kafka topics on {TARGET_HOST}...")
    result = subprocess.run(
        [os.path.join(SCRIPT_DIR, "manage-external-kafka.sh"), "reset"],
        env=script_env(),
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        log(f"Warning: Kafka reset may have failed:\n{result.stderr}")
    else:
        log("Kafka topics reset (empty).")


def start_producer(
    first_event_id: int,
    events: int,
    tps: int,
    parallelism: int,
    max_emit_speed: str,
    producer_rest_port: int,
) -> None:
    log(
        f"Starting producer on {TARGET_HOST}: first-event-id={first_event_id}, "
        f"events={events}, tps={tps}, parallelism={parallelism}, "
        f"max-emit-speed={max_emit_speed}, rest-port={producer_rest_port}"
    )
    env = script_env()
    env["FIRST_EVENT_ID"] = str(first_event_id)
    env["EVENTS"] = str(events)
    env["TPS"] = str(tps)
    env["PARALLELISM"] = str(parallelism)
    env["MAX_EMIT_SPEED"] = max_emit_speed
    env["FLINK_REST_PORT"] = str(producer_rest_port)
    result = subprocess.run(
        [os.path.join(SCRIPT_DIR, "manage-standalone-producer.sh"), "start"],
        env=env, capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        log(f"Error starting producer:\n{result.stderr}")
    else:
        log("Producer started.")


def setup_port_forward(deployment: str, port: int) -> subprocess.Popen:
    """Start kubectl port-forward to the Flink JobManager REST API."""
    if flink_rest_healthy(port):
        log(f"Flink REST already reachable on localhost:{port}; reusing existing port-forward.")
        return None

    log(f"Setting up port-forward to {deployment} on port {port}...")
    log_path = os.path.join(SCRIPT_DIR, ".scaling-kafka-coordinator-port-forward.log")
    log_file = open(log_path, "ab", buffering=0)
    proc = subprocess.Popen(
        [
            "bash", "-lc",
            (
                f"source {CLUSTER_ENV} >/dev/null 2>&1 && "
                f"exec kubectl port-forward svc/{deployment}-rest {port}:8081"
            ),
        ],
        stdout=log_file, stderr=log_file,
        start_new_session=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("kubectl port-forward exited immediately")
        if flink_rest_healthy(port):
            log(f"Port-forward established on localhost:{port}.")
            return proc
        time.sleep(1)
    proc.terminate()
    raise RuntimeError("kubectl port-forward did not become healthy")


def stop_port_forward(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def ensure_port_forward(
    port_forward_proc: subprocess.Popen | None,
    deployment: str,
    port: int,
) -> subprocess.Popen:
    if flink_rest_healthy(port):
        return port_forward_proc

    needs_restart = True
    if not needs_restart:
        return port_forward_proc

    if port_forward_proc is not None and port_forward_proc.poll() is None:
        log("Flink REST unhealthy; restarting port-forward.")
    stop_port_forward(port_forward_proc)
    return setup_port_forward(deployment, port)


def wait_for_job(port: int, timeout: int = 300) -> str:
    """Wait until we can discover a job id."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            job = get_active_job(port)
            if job:
                return job["jid"]
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError("Timed out waiting for Flink job to appear")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tps", type=int, default=50000, help="Events per second for the producer (default: 50000)")
    parser.add_argument("--events", type=int, default=300000000, help="Total events to produce (default: 300000000)")
    parser.add_argument("--parallelism", type=int, default=1, help="Producer parallelism (default: 1)")
    parser.add_argument(
        "--max-emit-speed",
        default="true",
        choices=("true", "false"),
        help="Pass through to the producer job (default: true)",
    )
    parser.add_argument(
        "--producer-rest-port",
        type=int,
        default=int(os.environ.get("PRODUCER_REST_PORT", os.environ.get("FLINK_REST_PORT", "18081"))),
        help="REST port for the standalone producer on the target host (default: 18081)",
    )
    parser.add_argument("--deployment", default="flink", help="FlinkDeployment name (default: flink)")
    parser.add_argument("--flink-port", type=int, default=8081, help="Local port for Flink REST API (default: 8081)")
    parser.add_argument("--no-port-forward", action="store_true", help="Skip automatic kubectl port-forward")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="Status poll interval in seconds (default: 2.0)")
    args = parser.parse_args()

    port_forward_proc = None
    if not args.no_port_forward:
        port_forward_proc = setup_port_forward(args.deployment, args.flink_port)

    try:
        log("Waiting for Flink job to appear...")
        job_id = wait_for_job(args.flink_port)
        active_job = get_active_job(args.flink_port)
        job_name = active_job.get("name") if active_job else None
        log(f"Tracking job: {job_id}")
        if job_name:
            log(f"Tracking job name: {job_name}")

        scaling_in_progress = False

        # Track the CREATED timestamp — it changes every time the adaptive
        # scheduler rescales (RUNNING -> CREATED -> RUNNING, often in <100ms).
        # Polling for transient status changes misses these fast transitions,
        # so we compare timestamps instead.
        last_created_ts = 0
        try:
            detail = get_job_detail(args.flink_port, job_id)
            last_created_ts = get_created_timestamp(detail)
            log(f"Initial CREATED timestamp: {last_created_ts}")
        except Exception:
            pass

        while True:
            if not args.no_port_forward:
                try:
                    port_forward_proc = ensure_port_forward(
                        port_forward_proc, args.deployment, args.flink_port
                    )
                except Exception as e:
                    log(f"Error maintaining port-forward: {e}")
                    time.sleep(args.poll_interval)
                    continue

            active_job = None
            try:
                active_job = get_active_job(args.flink_port, job_name)
            except Exception as e:
                if not scaling_in_progress:
                    scaling_in_progress = True
                    log(f"=== SCALING DETECTED === (cannot poll active jobs: {e})")
                    pause_producer()
                    stop_producer()
                    reset_kafka()
                time.sleep(args.poll_interval)
                continue

            if active_job is None:
                if not scaling_in_progress:
                    scaling_in_progress = True
                    log("=== SCALING DETECTED === (no active job currently visible)")
                    pause_producer()
                    stop_producer()
                    reset_kafka()
                time.sleep(args.poll_interval)
                continue

            current_job_id = active_job["jid"]
            if current_job_id != job_id and not scaling_in_progress:
                scaling_in_progress = True
                log(f"=== SCALING DETECTED === (job id changed: {job_id} -> {current_job_id})")
                pause_producer()
                stop_producer()
                reset_kafka()

            job_id = current_job_id

            try:
                detail = get_job_detail(args.flink_port, job_id)
                status = detail.get("state", active_job.get("state", "UNKNOWN"))
                created_ts = get_created_timestamp(detail)
            except Exception as e:
                if not scaling_in_progress:
                    scaling_in_progress = True
                    log(f"=== SCALING DETECTED === (lost tracked job detail: {e})")
                    pause_producer()
                    stop_producer()
                    reset_kafka()
                time.sleep(args.poll_interval)
                continue

            # Detect scaling: CREATED timestamp changed (adaptive scheduler rescaled)
            if created_ts > last_created_ts and not scaling_in_progress:
                scaling_in_progress = True
                from datetime import datetime as dt
                old_dt = dt.fromtimestamp(last_created_ts / 1000, tz=timezone.utc).strftime("%H:%M:%S") if last_created_ts > 0 else "N/A"
                new_dt = dt.fromtimestamp(created_ts / 1000, tz=timezone.utc).strftime("%H:%M:%S")
                log(f"=== SCALING DETECTED === (CREATED timestamp changed: {old_dt} -> {new_dt})")

                # Log current parallelism
                for v in detail.get("vertices", []):
                    log(f"  Vertex {v['id'][:10]} parallelism={v['parallelism']}")

                # Step 1: Pause the producer immediately
                pause_producer()

                # Step 2: Stop producer and reset Kafka
                stop_producer()
                reset_kafka()

            # After scaling actions are done, wait for RUNNING then restart producer
            last_created_ts = max(last_created_ts, created_ts)

            if scaling_in_progress and status == "RUNNING":
                scaling_in_progress = False
                last_created_ts = created_ts
                log(f"=== JOB RECOVERED === (job id {job_id})")

                for v in detail.get("vertices", []):
                    log(f"  Vertex {v['id'][:10]} parallelism={v['parallelism']}")

                # Step 3: Restart the full Nexmark run from scratch
                first_event_id = 1
                events = args.events
                start_producer(
                    first_event_id,
                    events,
                    args.tps,
                    args.parallelism,
                    args.max_emit_speed,
                    args.producer_rest_port,
                )
                log(f"Producer restarted from scratch: first-event-id={first_event_id}, events={events}")

            # Detect terminal states
            if status in ("FINISHED", "CANCELED", "FAILED", "SUSPENDED"):
                log(f"Job reached terminal state: {status}. Exiting.")
                break

            time.sleep(args.poll_interval)

    except KeyboardInterrupt:
        log("Interrupted by user.")
    finally:
        if port_forward_proc:
            stop_port_forward(port_forward_proc)
            log("Port-forward terminated.")


if __name__ == "__main__":
    main()
