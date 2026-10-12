"""Emit OpenLineage RunEvents for a asset-manager fund NAV flow, one JSON per line.

These are the events Airflow / Informatica / Tableau emit with the OpenLineage Kafka transport.
Pipe into Kafka:   python3 producer.py | kafka-console-producer.sh --topic openlineage.events ...
Or write a file:   python3 producer.py --out events.jsonl  (for the UC Volume / Auto Loader path)
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone

CATALOG = os.environ.get("DEMO_CATALOG", "main")
SCHEMA = "ext_lineage_demo"
DBX = "databricks://" + os.environ.get("DEMO_HOST", "<workspace-host>.cloud.databricks.com")


def uc(table):
    return f"{CATALOG}.{SCHEMA}.{table}"


def schema(*cols):
    return {"schema": {"fields": [{"name": c, "type": "string"} for c in cols]}}


def col_lineage(mapping, ns, name):
    """mapping: {target_col: source_col}"""
    return {"columnLineage": {"fields": {
        t: {"inputFields": [{"namespace": ns, "name": name, "field": s}]} for t, s in mapping.items()
    }}}


def ds(ns, name, facets=None, **extra):
    f = dict(facets or {})
    if "description" in extra:
        f["documentation"] = {"description": extra["description"]}
    if "url" in extra:
        f["demo_url"] = {"url": extra["url"]}
    if "entity" in extra:
        f["demo_entityType"] = {"type": extra["entity"]}
    return {"namespace": ns, "name": name, "facets": f}


def event(job_ns, job_name, producer, inputs, outputs):
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": producer,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {"runId": str(uuid.uuid4())},
        "job": {"namespace": job_ns, "name": job_name},
        "inputs": inputs,
        "outputs": outputs,
    }


KAFKA_NS = "kafka://msk-prod.example.internal:9092"
ORA_NS = "oracle://ora-prod.example.internal:1521/PORTFOLIO"
SNOW_NS = "snowflake://example-refdata.snowflakecomputing.com"
TABLEAU_NS = "tableau://tableau.example.internal"
PBI_NS = "powerbi://app.powerbi.com/groups/advisor-analytics"
SF_NS = "salesforce://example.my.salesforce.com"


def events():
    trade_cols = ["trade_id", "account_id", "fund_id", "side", "quantity", "price", "trade_ts"]
    yield event(
        "airflow://airflow-prod", "trade_capture.kafka_to_bronze",
        "https://github.com/OpenLineage/OpenLineage/tree/1.24.0/integration/airflow",
        [ds(KAFKA_NS, "trades.executions", schema(*trade_cols),
            description="Order-management system trade executions (Avro)", entity="Topic")],
        [ds(DBX, uc("bronze_trades"), col_lineage({c: c for c in trade_cols}, KAFKA_NS, "trades.executions"))],
    )
    yield event(
        "informatica://iics-prod", "wf_load_fund_holdings",
        "https://informatica.com/openlineage/iics",
        [ds(ORA_NS, "PORTFOLIO.FUND_HOLDINGS", schema("FUND_ID", "CUSIP", "SHARES_HELD", "AS_OF_DT"),
            description="Book-of-record fund holdings", entity="Table")],
        [ds(DBX, uc("bronze_holdings"), col_lineage(
            {"fund_id": "FUND_ID", "cusip": "CUSIP", "shares_held": "SHARES_HELD", "as_of_date": "AS_OF_DT"},
            ORA_NS, "PORTFOLIO.FUND_HOLDINGS"))],
    )
    yield event(
        "airflow://airflow-prod", "refdata.snowflake_prices_sync",
        "https://github.com/OpenLineage/OpenLineage/tree/1.24.0/integration/airflow",
        [ds(SNOW_NS, "REFDATA.MARKET.SECURITY_PRICES", schema("CUSIP", "CLOSE_PX", "PRICE_DT"),
            description="Vendor end-of-day security prices", entity="Table")],
        [ds(DBX, uc("bronze_prices"), col_lineage(
            {"cusip": "CUSIP", "close_price": "CLOSE_PX", "price_date": "PRICE_DT"},
            SNOW_NS, "REFDATA.MARKET.SECURITY_PRICES"))],
    )
    # downstream consumers of the gold table
    yield event(
        "tableau://tableau.example.internal", "extract_refresh.fund_nav_daily",
        "https://tableau.com/openlineage",
        [ds(DBX, uc("gold_fund_nav"))],
        [ds(TABLEAU_NS, "Fund Operations/Fund NAV Daily", {**schema("fund_id", "nav_date", "nav_per_share", "total_aum"),
            **col_lineage({c: c for c in ("fund_id", "nav_date", "nav_per_share", "total_aum")}, DBX, uc("gold_fund_nav"))},
            description="Daily NAV dashboard for Fund Accounting", entity="Dashboard",
            url="https://tableau.example.internal/#/views/FundNAVDaily")],
    )
    yield event(
        "powerbi://app.powerbi.com", "dataset_refresh.advisor_portfolio",
        "https://powerbi.microsoft.com/openlineage",
        [ds(DBX, uc("gold_fund_nav"))],
        [ds(PBI_NS, "Advisor Portfolio Report", {**schema("fund_id", "total_aum"),
            **col_lineage({"fund_id": "fund_id", "total_aum": "total_aum"}, DBX, uc("gold_fund_nav"))},
            description="Financial-advisor AUM report", entity="Report",
            url="https://app.powerbi.com/groups/advisor-analytics/reports/advisor-portfolio")],
    )
    yield event(
        "airflow://airflow-prod", "crm.push_fund_nav_to_salesforce",
        "https://github.com/OpenLineage/OpenLineage/tree/1.24.0/integration/airflow",
        [ds(DBX, uc("gold_fund_nav"))],
        [ds(SF_NS, "Fund_Performance__c", {**schema("Fund_Id__c", "NAV__c", "AUM__c"),
            **col_lineage({"Fund_Id__c": "fund_id", "NAV__c": "nav_per_share", "AUM__c": "total_aum"}, DBX, uc("gold_fund_nav"))},
            description="CRM fund performance object", entity="Object")],
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", help="write JSONL to a file instead of stdout")
    a = ap.parse_args()
    fh = open(a.out, "w") if a.out else sys.stdout
    for e in events():
        fh.write(json.dumps(e) + "\n")
    fh.flush()
