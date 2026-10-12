"""Kafka schema watcher: infer each topic's JSON schema, version it, publish changes as OpenLineage DatasetEvents.

  python3.11 schema_watcher.py [topic ...]     (default: oms.orders.raw trades.executions trades.fund_flows)

Detection rules (per topic):
  * added field      -> new version immediately
  * type change      -> new version immediately (nulls are ignored for typing)
  * removed field    -> new version after REMOVE_AFTER consecutive messages without it (tolerates optional fields)
  * always-null field -> reported as type change `<type>->null` after REMOVE_AFTER consecutive nulls
                        (schema shape unchanged but the data broke, e.g. an upstream rename)
State is persisted in .kafka/schema_state.json, so a restart neither re-emits nor forgets versions.
With a Schema Registry, replace infer() with a poll of /subjects/<topic>-value/versions/latest; the rest is the same.
"""
import json
import os
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
KAFKA = ROOT / ".kafka/kafka_2.13-3.8.0/bin"
STATE_FILE = ROOT / ".kafka/schema_state.json"
BOOT = "localhost:9092"
NAMESPACE = f"kafka://{BOOT}"
LINEAGE_TOPIC = "openlineage.events"
REMOVE_AFTER = 5
ENV = {**os.environ, "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk")}

lock = threading.Lock()
state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
producer = subprocess.Popen([str(KAFKA / "kafka-console-producer.sh"), "--bootstrap-server", BOOT, "--topic", LINEAGE_TOPIC],
                            stdin=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=ENV)


def json_type(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "long"
    if isinstance(v, float):
        return "double"
    if isinstance(v, dict):
        return "struct"
    if isinstance(v, list):
        return "array"
    return "string"


def infer(msg: dict) -> dict:
    return {k: json_type(v) for k, v in msg.items()}


def emit(topic: str, st: dict, added, removed, type_changed):
    parts = [f"+{a}" for a in added] + [f"-{r}" for r in removed] + [f"~{t}" for t in type_changed]
    st.setdefault("history", []).append(f"v{st['version']}: " + (" ".join(parts) if st["version"] > 1 else "baseline"))
    st["history"] = st["history"][-10:]
    event = {
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": "https://github.com/example/kafka-schema-watcher",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent",
        "dataset": {
            "namespace": NAMESPACE,
            "name": topic,
            "facets": {
                "schema": {"fields": [{"name": f, "type": t} for f, t in st["fields"].items()]},
                "demo_entityType": {"type": "Topic"},
                "demo_schemaChange": {
                    "version": st["version"], "added": added, "removed": removed,
                    "type_changed": type_changed, "changed_at": datetime.now(timezone.utc).isoformat(),
                    "history": " | ".join(st["history"]),
                },
            },
        },
    }
    producer.stdin.write(json.dumps(event) + "\n")
    producer.stdin.flush()
    STATE_FILE.write_text(json.dumps(state, indent=1))
    print(f"[{topic}] schema v{st['version']}  added={added} removed={removed} type_changed={type_changed}", flush=True)


def observe(topic: str, msg: dict):
    obs = infer(msg)
    with lock:
        st = state.get(topic)
        if st is None:  # first sighting -> baseline
            state[topic] = st = {"version": 1, "fields": {k: t or "string" for k, t in obs.items()}, "missing": {}}
            emit(topic, st, list(obs), [], [])
            return
        fields, missing, nulls = st["fields"], st["missing"], st.setdefault("nulls", {})
        added = [k for k in obs if k not in fields]
        type_changed = [f"{k}:{fields[k]}->{t}" for k, t in obs.items() if k in fields and t and t != fields[k]]
        for k, t in obs.items():
            nulls[k] = nulls.get(k, 0) + 1 if (k in fields and t is None) else 0
            if nulls[k] == REMOVE_AFTER and fields[k] != "null":
                type_changed.append(f"{k}:{fields[k]}->null")
        removed = []
        for k in list(fields):
            missing[k] = 0 if k in obs else missing.get(k, 0) + 1
            if missing[k] >= REMOVE_AFTER:
                removed.append(k)
        if not (added or type_changed or removed):
            return
        for k in added:
            fields[k] = obs[k] or "string"
        for k, t in obs.items():
            if k in fields and t:
                fields[k] = t
            elif k in fields and nulls.get(k) == REMOVE_AFTER:
                fields[k] = "null"
        for k in removed:
            fields.pop(k)
            missing.pop(k, None)
        st["version"] += 1
        emit(topic, st, added, removed, type_changed)


def watch(topic: str):
    proc = subprocess.Popen([str(KAFKA / "kafka-console-consumer.sh"), "--bootstrap-server", BOOT, "--topic", topic,
                             "--group", "uc-schema-watcher", "--from-beginning"],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=ENV)
    for line in proc.stdout:
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if isinstance(msg, dict):
            observe(topic, msg)


if __name__ == "__main__":
    topics = sys.argv[1:] or ["oms.orders.raw", "trades.executions", "trades.fund_flows"]
    print(f"watching {topics}; schema changes -> {LINEAGE_TOPIC}", flush=True)
    threads = [threading.Thread(target=watch, args=(t,), daemon=True) for t in topics]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
