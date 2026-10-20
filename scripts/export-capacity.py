"""Export credential-free acceptance results; never select fixture files."""
import argparse
import csv
import gzip
import json
import re
import statistics
from pathlib import Path


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def memory_mib(value):
    match = re.match(r"([0-9.]+)\s*([a-zA-Z]+)", value)
    if not match:
        return None
    factors = {"B": 1 / 1048576, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024,
               "kB": 1000 / 1048576, "MB": 1000000 / 1048576, "GB": 1000000000 / 1048576}
    return float(match[1]) * factors.get(match[2], 0)


def summarize_report(path):
    raw = read(path)
    database = raw["database"]
    outcomes = raw["http_outcomes"]
    rejected = outcomes.get("workflow_rejected", 0) + outcomes.get("chat_rejected", 0)
    completed = database["workflow_states"].get("succeeded", 0) + database["chat_states"].get("done", 0)
    received = database["workflow_total"] + database["chat_total"]
    gaps = list(raw.get("gaps") or [])
    if rejected and not any("rejected" in gap for gap in gaps):
        gaps.append("offered traffic was rejected; not full offered-rate capacity")
    if raw.get("wire_after_offer_window", 0) and not any("window" in gap for gap in gaps):
        gaps.append("actual wire delivery extended past the offer window")
    missing_terminal = outcomes.get("chat_missing_terminal", 0)
    if missing_terminal:
        gaps.append("client did not receive Chat terminal")
    client_errors = sum(outcomes.get(name, 0) for name in (
        "chat_http_error", "chat_transport_unknown", "chat_failed", "chat_missing_terminal",
        "workflow_transport_or_http_unknown", "client_sse_slot_error", "workflow_subscription_error"))
    if client_errors:
        gaps.append("client error outcomes recorded; inspect independently of DB success")
    samples = raw.get("resource_samples", [])
    observation_errors = sum(bool(sample.get("error")) for sample in samples)
    cpu, working_set = {}, {}
    for sample in samples:
        for row in sample.get("docker_stats", []):
            name = row["Name"]
            try:
                cpu[name] = max(cpu.get(name, 0), float(row["CPUPerc"].rstrip("%")) / 100)
            except ValueError:
                pass
            memory = memory_mib(row.get("MemUsage", ""))
            if memory is not None:
                working_set[name] = max(working_set.get(name, 0), memory)
    load_samples = [sample for sample in samples if sample["Phase"] == "load"]
    idle_samples = [sample for sample in samples if sample["Phase"] == "idle"]
    idle = {}
    for process in ("Server", "Worker"):
        tail = [sample[process] for sample in idle_samples[-12:] if sample.get(process)]
        if tail:
            idle[process.lower()] = {
                "tail_samples": len(tail),
                "median_goroutines": statistics.median(row["goroutines"] for row in tail),
                "median_heap_alloc_mib": statistics.median(row["heap_alloc"] / 1048576 for row in tail),
                "last_heap_sys_mib": tail[-1]["heap_sys"] / 1048576,
                "last_database_pool": tail[-1].get("database_pool"),
            }
    if idle_samples:
        idle["last_outbox"] = idle_samples[-1]["Outbox"]
        idle["last_outstanding"] = idle_samples[-1]["Outstanding"]
    return {
        "name": path.stem, "config": raw["config"], "started": raw["Started"], "finished": raw["Finished"],
        "arrivals": raw["arrivals"], "actual_wire": raw["actual_wire_requests"],
        "wire_after_window": raw.get("wire_after_offer_window"),
        "wire_per_second": raw.get("wire_per_second"),
        "received": received, "completed": completed, "rejected": rejected,
        "database_non_success": received - completed,
        "workflow_states": database["workflow_states"], "chat_states": database["chat_states"],
        "http_outcomes": outcomes, "client_errors": client_errors,
        "unacknowledged_workflows": database["unacknowledged_workflows"],
        "peer_mismatches": raw["peer_completed_run_mismatches"],
        "latency": raw["latency"],
        "queue": database["workflow_queue"], "execution": database["workflow_execution"],
        "workflow_end_to_end": database["workflow_end_to_end"],
        "original_passed": raw["passed"], "reconciled_passed": not gaps,
        "sse_opened": raw.get("SSEOpened"), "sse_failures": raw.get("SSEFailures"),
        "sse_setup_failures": raw.get("SSESetupFailures"), "sse_failure_causes": raw.get("sse_failure_causes"),
        "gaps": gaps, "observation_errors": observation_errors,
        "actual_sse_load_min": min((s["SSE"] for s in load_samples), default=None),
        "actual_sse_load_max": max((s["SSE"] for s in load_samples), default=None),
        "actual_sse_load_median": statistics.median(s["SSE"] for s in load_samples) if load_samples else None,
        "peak_cpu_cores": cpu, "peak_working_set_mib": working_set,
        "peak_outbox": max((s["Outbox"] for s in samples), default=None),
        "idle": idle,
    }, raw


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("campaign", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    source, destination = args.campaign.resolve(), args.destination.resolve()
    files = sorted({*source.glob("grid*.json"), *source.glob("baseline*.json"), *source.glob("recovery*.json"), *source.glob("worker-kill.json")})
    if not files:
        raise SystemExit("no acceptance reports; fixture files are never selected")
    destination.mkdir(parents=True, exist_ok=True)
    summaries = []
    for path in files:
        summary, raw = summarize_report(path)
        summaries.append(summary)
        # No tokens/credentials appear in the report schema. Guard accidental
        # schema expansion before publishing raw data, rather than trusting a glob.
        serialized = json.dumps(raw, ensure_ascii=False, separators=(",", ":")).encode()
        if b'"access_token"' in serialized or b'"refresh_token"' in serialized or b'Bearer ' in serialized:
            raise SystemExit("unexpected credentials in report; export refused")
        (destination / (path.name + ".gz")).write_bytes(gzip.compress(serialized, mtime=0))
    (destination / "summary.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")
    columns = ["name", "sse", "slots", "rps", "wire", "completed", "rejected", "db_non_success", "wire_after_window", "query_p95_ms", "query_p99_ms", "approve_p95_ms", "cancel_p95_ms", "passed"]
    with (destination / "matrix.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in summaries:
            latency = row["latency"]
            writer.writerow(dict(zip(columns, [row["name"], row["config"]["SSE"], row["config"]["Slots"], row["config"]["Rate"], row["actual_wire"], row["completed"], row["rejected"], row["database_non_success"], row["wire_after_window"], latency.get("control_query", {}).get("p95_ms"), latency.get("control_query", {}).get("p99_ms"), latency.get("control_approve", {}).get("p95_ms"), latency.get("control_cancel", {}).get("p95_ms"), row["reconciled_passed"]])))
    for name in ("manifest.json", "environment.json"):
        for path in (source / name, source.parent / name):
            if path.is_file():
                (destination / name).write_text(json.dumps(read(path), ensure_ascii=False, indent=2), encoding="utf-8")
                break
    print(f"exported {len(summaries)} reports; no fixture files selected")


if __name__ == "__main__":
    main()
