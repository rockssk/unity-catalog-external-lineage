# Automating external lineage into Unity Catalog

End-to-end demo: lineage and schema metadata from systems **outside** Databricks (Kafka, Oracle, Snowflake, Airflow,
Informatica, Tableau, Power BI, Salesforce) flows automatically into Unity Catalog through the
**External Metadata** and **External Lineage** APIs (Public Preview). The result is one lineage graph in Catalog Explorer.

```
Airflow / Informatica / Kafka Streams / schema watcher ──OpenLineage──▶ Kafka topic `openlineage.events`
                                                                          │
                     lineage agent (local)  ─or─  Databricks job (notebooks/openlineage_kafka_ingest.py)
                                                                          │  lineage_bridge
                                                                          ▼
          UC External Metadata objects + External Lineage relationships (table + column level)
```

Scenario: an asset manager's fund NAV pipeline.

| Upstream (external) | → Databricks (`<catalog>.ext_lineage_demo`) | Downstream (external) |
|---|---|---|
| Kafka `oms.orders.raw` → *Kafka Streams `trade-enricher`* → `trades.executions` | `bronze_trades` | |
| Oracle `PORTFOLIO.FUND_HOLDINGS` (Informatica) | `bronze_holdings` | |
| Snowflake `REFDATA.MARKET.SECURITY_PRICES` (Airflow) | `bronze_prices` | |
| | `silver_positions` → `gold_fund_nav` (native UC lineage) | Tableau dashboard, Power BI report, Salesforce object |

## What it demonstrates
1. **OpenLineage → UC.** `lineage_bridge` maps OpenLineage RunEvents and DatasetEvents to UC objects and edges. Writes are idempotent, so a topic can be replayed safely.
2. **Kafka transformations are captured automatically.** `trade-enricher` is a real Kafka Streams app. At startup it walks its own
   `Topology.describe()` and publishes lineage: source and sink topics, column mappings taken from the same field map the
   transform uses, and the processor chain stored as a `transformation` property on each UC edge.
3. **Kafka schema changes reach UC metadata.** `schema_watcher.py` infers and versions each topic's JSON schema and publishes
   DatasetEvents on change. UC columns and the `schema`, `schema_version`, `schema_history` and `schema_stale_column_mappings` properties
   update within seconds.
4. **Databricks-native ingest.** A notebook reads lineage events from Kafka (MSK/Confluent, SASL via a secret scope) or from a UC Volume,
   lands them in an audit table, and applies them to UC.

## Prerequisites
- A workspace whose metastore lets you create external metadata: metastore admin, or
  ``GRANT CREATE EXTERNAL METADATA ON METASTORE TO `<you>`;``
- Databricks CLI profile; Python 3.11 + `databricks-sdk>=0.70`; Java 17+ (`JAVA_HOME`) for local Kafka.
  No Kafka Python client is needed, because the Kafka CLI tools are piped.

```bash
export DEMO_CATALOG=<catalog> DEMO_HOST=<workspace-host>.cloud.databricks.com
PROFILE=<cli-profile>
```

## Run
```bash
python3.11 run_sql.py sql/setup_uc.sql $PROFILE              # demo tables (native bronze→silver→gold lineage)
scripts/kafka.sh up                                          # local KRaft broker on :9092 + topics (downloads Kafka 3.8)
scripts/kafka.sh agent $PROFILE &                            # always-on: openlineage.events → UC
scripts/kafka.sh streams-build && scripts/kafka.sh streams-run &   # Kafka Streams app publishes its own lineage
scripts/kafka.sh watcher &                                   # always-on: topic schema changes → openlineage.events
python3.11 producer.py | scripts/kafka.sh produce            # lineage from Airflow / Informatica / BI (simulated)
python3.11 orders_generator.py --n 20 | scripts/kafka.sh produce oms.orders.raw
```
Open `<catalog>.ext_lineage_demo.gold_fund_nav` → **Lineage** in Catalog Explorer.

### Simulate upstream schema changes
```bash
python3.11 orders_generator.py --n 20 --schema-version 2 | scripts/kafka.sh produce oms.orders.raw   # +venue +order_type
python3.11 orders_generator.py --n 25 --schema-version 3 | scripts/kafka.sh produce oms.orders.raw   # acct→account_ref, px→string
```
Expected result on the UC objects (`trade-enricher` passes unmapped fields through, so changes cascade):
- `oms.orders.raw`: `v1: baseline | v2: +venue +order_type | v3: +account_ref ~px:double->string | v4: -acct`, plus
  `schema_stale_column_mappings: acct->account_id`. UC does not validate edge mappings against columns, so the bridge flags them.
- `trades.executions`: `… | v3: +account_ref ~price:double->string | v4: ~account_id:string->null`. The shape is unchanged but the data broke.

| Watcher rule | Trigger |
|---|---|
| added field | immediately |
| type change | immediately (nulls ignored) |
| removed field | after 5 consecutive messages without it |
| field always null | after 5 consecutive nulls → `type->null` |

The watcher is the authority on columns. Job events only fill descriptive fields, so a job restart can't roll back a detected change.
With a Schema Registry, replace `infer()` with a poll of `/subjects/<topic>-value/versions/latest`.

### Databricks-native ingest (job)
Upload `lineage_bridge/` + `notebooks/` to `/Workspace/Users/<your-user>/unity-catalog-external-lineage`, then
`databricks jobs submit --json @jobs/ingest_volume_run.json --profile $PROFILE`.
- `source=kafka`: set `bootstrap_servers`, plus an optional `kafka_secret_scope` with `username`/`password` keys
- `source=volume`: `databricks fs cp events.jsonl dbfs:/Volumes/<catalog>/ext_lineage_demo/landing/openlineage/`

Notes: serverless ships an older `databricks-sdk`, so the job environment pins `>=0.70.0`. Lineage is applied on the driver
after an `availableNow` stream lands the events. It doesn't run in `foreachBatch`, because serverless pickles that function into a separate process.

## Mapping rules (`lineage_bridge/bridge.py`)
- Namespace scheme → `system_type` (`kafka`→KAFKA, `oracle`→ORACLE, `snowflake`→SNOWFLAKE, `tableau`→TABLEAU, `powerbi`→POWER_BI, …; unknown→OTHER)
- `databricks://…` datasets → UC tables. Databricks↔Databricks edges are skipped, because UC captures them natively.
- `schema` facet → columns (+ typed `schema` property); `columnLineage` facet → column mappings
- Job facets → edge properties `processing_type`, `transformation`, `openlineage_job`, `openlineage_run_id`
- External metadata names are global to the metastore, so all objects use the prefix `extlin__`

## API gotchas found while building
- `list_external_lineage_relationships` returns the *other* endpoint in `table_info` / `external_metadata_info`; the relationship itself has no source/target.
- Kafka→Kafka (external→external) relationships are supported.
- UC does not validate relationship column mappings against external metadata columns.

## Downstream lineage
OpenLineage covers writers. BI tools and other readers rarely emit it. Combine:
1. **Databricks telemetry**: join `system.access.table_lineage` (reads, `statement_id`) with `system.query.history`
   (`client_application`, `executed_by`, `query_tags`) to find which system reads each table. Use one service principal per tool.
2. **Downstream tool APIs** (asset level): Tableau Metadata API, Power BI Scanner API, Looker API, reverse-ETL sync APIs.
3. **Reconcile both**: reads without API lineage point to an undocumented consumer; lineage without reads points to a dead dashboard.

## Cleanup
`python3.11 cleanup.py $PROFILE` (deletes `extlin__*` objects + relationships) · `scripts/kafka.sh down` ·
`DROP SCHEMA <catalog>.ext_lineage_demo CASCADE`. Reset for a rehearsal: `scripts/kafka.sh demo-reset`.

## Layout
```
lineage_bridge/bridge.py         OpenLineage → UC External Metadata / External Lineage
consumer.py                      stdin/file JSONL → bridge (used by the agent)
producer.py                      simulated OpenLineage RunEvents (Airflow, Informatica, BI)
orders_generator.py              raw OMS orders, schema versions 1–3
schema_watcher.py                Kafka topic schema inference + DatasetEvents
streams/src/demo/                Kafka Streams app + topology → OpenLineage emitter
notebooks/openlineage_kafka_ingest.py   Databricks job: Kafka/Volume → audit table → UC
scripts/kafka.sh                 local Kafka, agent, watcher, streams, reset
sql/setup_uc.sql, run_sql.py     demo tables
cleanup.py                       remove demo objects
```

The UC External Metadata and External Lineage APIs are in Public Preview and may change.
