"""Local Kafka -> UC External Lineage consumer.

Reads OpenLineage JSON lines from stdin (piped from kafka-console-consumer) or a file and
applies them with LineageBridge.

  kafka-console-consumer.sh --topic openlineage.events --from-beginning ... \
    | python3 consumer.py --profile DEFAULT
"""
import argparse
import json
import sys

from databricks.sdk import WorkspaceClient

from lineage_bridge import LineageBridge

PREFIX = "extlin__"

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--file", help="read JSONL from a file instead of stdin")
    a = ap.parse_args()

    bridge = LineageBridge(WorkspaceClient(profile=a.profile), name_prefix=PREFIX,
                           log=lambda m: print(m, flush=True))
    src = open(a.file) if a.file else sys.stdin
    total = 0
    for line in src:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            total += bridge.process_event(json.loads(line))
        except Exception as e:  # keep consuming; one bad event must not stop the stream
            print(f"  ! failed: {e}", flush=True)
    print(f"done: {total} lineage edges applied", flush=True)
