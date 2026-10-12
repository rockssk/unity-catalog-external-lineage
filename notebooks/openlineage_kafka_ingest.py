# Databricks notebook source
# MAGIC %md
# MAGIC # OpenLineage (Kafka) → Unity Catalog External Lineage
# MAGIC Consumes OpenLineage RunEvents emitted by Airflow / Informatica / dbt / Tableau via the
# MAGIC OpenLineage **Kafka transport**, lands them in an audit table, and registers every external
# MAGIC system + edge in UC (External Metadata + External Lineage APIs). Lineage then shows up in
# MAGIC Catalog Explorer → *Lineage* alongside Databricks-native lineage.
# MAGIC
# MAGIC * `source = kafka` → `readStream.format("kafka")` against MSK / Confluent (set `bootstrap_servers`)
# MAGIC * `source = volume` → Auto Loader over JSONL files in a UC Volume (no broker needed)
# MAGIC
# MAGIC Run as a scheduled job (`availableNow`). Job environment needs `databricks-sdk>=0.70.0`.

# COMMAND ----------

dbutils.widgets.dropdown("source", "volume", ["kafka", "volume"])
dbutils.widgets.text("bootstrap_servers", "")
dbutils.widgets.text("topic", "openlineage.events")
dbutils.widgets.text("kafka_secret_scope", "")  # optional: SASL username/password keys
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ext_lineage_demo")
dbutils.widgets.text("name_prefix", "extlin__")

source = dbutils.widgets.get("source")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
landing = f"/Volumes/{catalog}/{schema}/landing"
checkpoint = f"{landing}/_checkpoints/openlineage_{source}"

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath(".."))  # repo root holds lineage_bridge/
from lineage_bridge import LineageBridge

# COMMAND ----------

from pyspark.sql import functions as F

if source == "kafka":
    opts = {
        "kafka.bootstrap.servers": dbutils.widgets.get("bootstrap_servers"),
        "subscribe": dbutils.widgets.get("topic"),
        "startingOffsets": "earliest",
    }
    scope = dbutils.widgets.get("kafka_secret_scope")
    if scope:
        user, pwd = dbutils.secrets.get(scope, "username"), dbutils.secrets.get(scope, "password")
        opts.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                                      f'required username="{user}" password="{pwd}";',
        })
    raw = spark.readStream.format("kafka").options(**opts).load().select(F.col("value").cast("string").alias("raw"))
else:
    raw = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "text")
           .load(f"{landing}/openlineage/")
           .select(F.col("value").alias("raw")))

events = raw.select(
    F.to_timestamp(F.get_json_object("raw", "$.eventTime")).alias("event_time"),
    F.concat_ws("/", F.get_json_object("raw", "$.job.namespace"), F.get_json_object("raw", "$.job.name")).alias("job"),
    F.get_json_object("raw", "$.run.runId").alias("run_id"),
    F.get_json_object("raw", "$.producer").alias("producer"),
    "raw",
    F.current_timestamp().alias("ingested_at"),
)

# COMMAND ----------

import json

# 1) stream: land raw events in the audit table (exactly-once via checkpoint)
run_start = spark.sql("SELECT current_timestamp() AS t").first().t
(events.writeStream
   .option("checkpointLocation", checkpoint)
   .trigger(availableNow=True)
   .toTable(f"{catalog}.{schema}.openlineage_events")
   .awaitTermination())

# 2) driver: apply only the events this run landed to UC. This step stays outside foreachBatch
#    because serverless pickles foreachBatch into a separate process (the SDK client can't go
#    along), and lineage volume is small anyway.
bridge = LineageBridge(name_prefix=dbutils.widgets.get("name_prefix"))
new_events = spark.table(f"{catalog}.{schema}.openlineage_events").where(F.col("ingested_at") >= F.lit(run_start))
edges = 0
for row in new_events.orderBy("event_time").select("raw").collect():
    try:
        edges += bridge.process_event(json.loads(row.raw))
    except Exception as e:
        print(f"  ! failed: {e}")
print(f"{edges} lineage edges applied")

# COMMAND ----------

# MAGIC %md ### What got registered

# COMMAND ----------

display(spark.sql(f"SELECT event_time, job, run_id, producer FROM {catalog}.{schema}.openlineage_events ORDER BY ingested_at DESC"))
