#!/usr/bin/env bash
# Local single-node Kafka (KRaft, no ZooKeeper) on localhost:9092.
#   scripts/kafka.sh up | down
#   scripts/kafka.sh produce [topic]                 stdin JSON lines -> topic (default openlineage.events)
#   scripts/kafka.sh consume [topic] [--from-beginning]   topic -> stdout, exits after 10s idle
#   scripts/kafka.sh streams-build | streams-run | streams-stop   Kafka Streams trade-enricher (emits its own lineage)
#   scripts/kafka.sh agent [profile]                 always-on: openlineage.events -> UC external lineage
#   scripts/kafka.sh watcher                         always-on: topic schema changes -> openlineage.events
#   scripts/kafka.sh demo-reset                      wipe data topics + watcher/streams state for a rehearsal
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KDIR="$ROOT/.kafka"
KAFKA="$KDIR/kafka_2.13-3.8.0"
LINEAGE_TOPIC="openlineage.events"
BOOT="localhost:9092"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk}"
export PATH="$JAVA_HOME/bin:$PATH"
topics() { "$KAFKA/bin/kafka-topics.sh" --bootstrap-server "$BOOT" "$@" 2>/dev/null; }
ALL_TOPICS="$LINEAGE_TOPIC oms.orders.raw trades.executions trades.fund_flows"
ensure_topics() {   # create missing topics and verify they are really there (topic deletion is async)
  for t in $ALL_TOPICS; do
    for _ in $(seq 1 20); do
      topics --list | grep -qx "$t" && break
      topics --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null || true
      sleep 1
    done
    topics --list | grep -qx "$t" || { echo "ERROR: topic $t could not be created" >&2; exit 1; }
  done
}

case "${1:-}" in
  up)
    if [ ! -d "$KAFKA" ]; then
      curl -sfL -o "$KDIR/kafka.tgz" https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz
      tar xzf "$KDIR/kafka.tgz" -C "$KDIR"
    fi
    CFG="$KDIR/server.properties"
    sed -e "s#^log.dirs=.*#log.dirs=$KDIR/data#" "$KAFKA/config/kraft/server.properties" > "$CFG"
    if [ ! -f "$KDIR/data/meta.properties" ]; then
      "$KAFKA/bin/kafka-storage.sh" format -t "$("$KAFKA/bin/kafka-storage.sh" random-uuid)" -c "$CFG" >/dev/null
    fi
    topics --list >/dev/null || "$KAFKA/bin/kafka-server-start.sh" -daemon "$CFG"
    for _ in $(seq 1 30); do topics --list >/dev/null && break; sleep 1; done
    ensure_topics
    echo "Kafka up on $BOOT; topics:"; topics --list
    ;;
  down)
    "$KAFKA/bin/kafka-server-stop.sh" || true
    ;;
  produce)
    "$KAFKA/bin/kafka-console-producer.sh" --bootstrap-server "$BOOT" --topic "${2:-$LINEAGE_TOPIC}" 2>/dev/null
    ;;
  consume)
    topic="$LINEAGE_TOPIC"; [ $# -ge 2 ] && [[ "$2" != --* ]] && { topic="$2"; shift; }; shift
    [[ " $* " == *" --timeout-ms "* ]] || set -- --timeout-ms 10000 "$@"
    "$KAFKA/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOT" --topic "$topic" "$@" 2>/dev/null || true
    ;;
  demo-reset)
    # fresh data topics + watcher/streams state (lineage topic and agent offsets are kept). Stop streams/watcher first.
    pkill -f 'java.*demo[.]TradeEnricher' 2>/dev/null || true; pkill -f 'schema_watcher[.]py' 2>/dev/null || true; sleep 3
    for t in oms.orders.raw trades.executions trades.fund_flows; do topics --delete --topic "$t" >/dev/null || true; done
    # `|| true`: with pipefail, grep finding no internal topics would otherwise abort the whole reset
    { topics --list | grep '^trade-enricher-' || true; } | while read -r t; do topics --delete --topic "$t" >/dev/null || true; done
    for g in uc-schema-watcher trade-enricher; do
      "$KAFKA/bin/kafka-consumer-groups.sh" --bootstrap-server "$BOOT" --delete --group "$g" >/dev/null 2>&1 || true
    done
    rm -rf "$KDIR/streams-state" "$KDIR/schema_state.json"
    for _ in $(seq 1 30); do   # wait until the broker has finished deleting (otherwise re-create is silently skipped)
      topics --list | grep -qE '^(oms\.orders\.raw|trades\.executions|trades\.fund_flows|trade-enricher-.*)$' || break; sleep 1
    done
    ensure_topics
    echo "data topics, schema-watcher state and streams state reset; topics:"; topics --list | grep -v '^__'
    ;;
  watcher)
    ensure_topics
    cd "$ROOT"; exec python3.11 -u schema_watcher.py
    ;;
  streams-stop)
    pkill -f 'java.*demo[.]TradeEnricher' && echo "trade-enricher stopped" || echo "trade-enricher not running"
    ;;
  streams-build)
    rm -rf "$ROOT/streams/classes" && mkdir -p "$ROOT/streams/classes"
    javac -d "$ROOT/streams/classes" -cp "$KAFKA/libs/*" "$ROOT"/streams/src/demo/*.java && echo "built streams/classes"
    ;;
  streams-run)
    ensure_topics   # before the instance check, so the check happens right before launch
    # one instance per state dir: stop any previous run (e.g. a background copy) before starting
    if pgrep -f 'java.*demo[.]TradeEnricher' >/dev/null; then
      echo "stopping running trade-enricher..."; pkill -f 'java.*demo[.]TradeEnricher' || true
      for _ in $(seq 1 30); do pgrep -f 'java.*demo[.]TradeEnricher' >/dev/null || break; sleep 1; done
      pkill -9 -f 'java.*demo[.]TradeEnricher' 2>/dev/null && sleep 1 || true
    fi
    cd "$ROOT"
    exec java -cp "$ROOT/streams/classes:$KAFKA/libs/*" -Dlog4j.configuration="file:$KAFKA/config/tools-log4j.properties" demo.TradeEnricher
    ;;
  agent)
    # long-running consumer group: every lineage event lands in UC within seconds; restarts resume from committed offsets
    cd "$ROOT"
    "$KAFKA/bin/kafka-console-consumer.sh" --bootstrap-server "$BOOT" --topic "$LINEAGE_TOPIC" \
      --group uc-lineage-agent --from-beginning 2>/dev/null \
      | python3.11 -u consumer.py --profile "${2:-DEFAULT}"
    ;;
  *) sed -n '2,8p' "$0"; exit 1 ;;
esac
