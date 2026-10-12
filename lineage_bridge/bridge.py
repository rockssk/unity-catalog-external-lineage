"""OpenLineage RunEvent -> Unity Catalog External Lineage.

For every COMPLETE run event:
  1. each non-Databricks dataset (inputs + outputs) is upserted as a UC External Metadata object
  2. each input -> output edge becomes an External Lineage relationship, with column
     mappings taken from the OpenLineage `columnLineage` facet

Databricks -> Databricks edges are skipped: UC already captures those natively.
All writes are idempotent, so replaying a Kafka topic from offset 0 is safe.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import AlreadyExists, NotFound, ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    ColumnRelationship,
    CreateRequestExternalLineage,
    ExternalLineageExternalMetadata,
    ExternalLineageObject,
    ExternalLineageTable,
    ExternalMetadata,
    LineageDirection,
    SystemType,
    UpdateRequestExternalLineage,
)

# OpenLineage namespace scheme -> (UC system type, default entity type)
SCHEME_MAP = {
    "kafka": (SystemType.KAFKA, "Topic"),
    "confluent": (SystemType.CONFLUENT, "Topic"),
    "oracle": (SystemType.ORACLE, "Table"),
    "snowflake": (SystemType.SNOWFLAKE, "Table"),
    "sqlserver": (SystemType.MICROSOFT_SQL_SERVER, "Table"),
    "mssql": (SystemType.MICROSOFT_SQL_SERVER, "Table"),
    "postgres": (SystemType.POSTGRESQL, "Table"),
    "postgresql": (SystemType.POSTGRESQL, "Table"),
    "mysql": (SystemType.MYSQL, "Table"),
    "redshift": (SystemType.AMAZON_REDSHIFT, "Table"),
    "bigquery": (SystemType.GOOGLE_BIGQUERY, "Table"),
    "teradata": (SystemType.TERADATA, "Table"),
    "mongodb": (SystemType.MONGODB, "Collection"),
    "salesforce": (SystemType.SALESFORCE, "Object"),
    "sap": (SystemType.SAP, "Table"),
    "tableau": (SystemType.TABLEAU, "Dashboard"),
    "powerbi": (SystemType.POWER_BI, "Report"),
    "looker": (SystemType.LOOKER, "Dashboard"),
}


def _scheme(namespace: str) -> str:
    return namespace.split("://", 1)[0].lower() if "://" in namespace else namespace.lower()


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", s).strip("_").lower()


@dataclass
class Dataset:
    namespace: str
    name: str
    facets: dict = field(default_factory=dict)

    @property
    def is_databricks(self) -> bool:
        return _scheme(self.namespace) == "databricks"

    @property
    def uc_name(self) -> str:
        """External metadata name (UC-unique) or the UC three-part table name."""
        if self.is_databricks:
            return self.name
        return _safe(f"{_scheme(self.namespace)}__{self.name}")

    def lineage_object(self) -> ExternalLineageObject:
        if self.is_databricks:
            return ExternalLineageObject(table=ExternalLineageTable(name=self.uc_name))
        return ExternalLineageObject(external_metadata=ExternalLineageExternalMetadata(name=self.uc_name))


class LineageBridge:
    def __init__(self, w: WorkspaceClient | None = None, name_prefix: str = "", log=print):
        self.w = w or WorkspaceClient()
        self.prefix = name_prefix
        self.log = log
        self._seen_metadata: dict[str, str] = {}  # name -> fingerprint of last write

    # ---------- external metadata ----------
    def _name(self, ds: Dataset) -> str:
        return ds.uc_name if ds.is_databricks else f"{self.prefix}{ds.uc_name}"

    def upsert_metadata(self, ds: Dataset, authoritative_schema: bool = False) -> None:
        """Create or update one external system object.

        Job (Run) events describe what a job *declares*; DatasetEvents from the schema watcher describe what
        is *observed* on the topic. Only the latter (authoritative_schema=True) may overwrite columns and
        schema properties of an existing object, so a job restart can't roll back a detected schema change.
        """
        name = self._name(ds)
        if ds.is_databricks:
            return
        scheme = _scheme(ds.namespace)
        system_type, entity_type = SCHEME_MAP.get(scheme, (SystemType.OTHER, "Dataset"))
        facets = ds.facets or {}
        entity_type = facets.get("demo_entityType", {}).get("type", entity_type)
        fields = facets.get("schema", {}).get("fields", [])
        columns = [f["name"] for f in fields] or None
        props = {"openlineage_namespace": ds.namespace, "openlineage_name": ds.name}
        if fields and all(f.get("type") for f in fields):
            props["schema"] = ", ".join(f"{f['name']}:{f['type']}" for f in fields)[:1000]
        change = facets.get("demo_schemaChange", {})
        for k in ("version", "added", "removed", "type_changed", "changed_at", "history"):
            if change.get(k) not in (None, "", []):
                v = change[k]
                props[f"schema_{k}"] = ", ".join(v) if isinstance(v, list) else str(v)
        if authoritative_schema and change.get("removed"):
            stale = self._stale_mappings(name, set(change["removed"]))
            if stale:
                props["schema_stale_column_mappings"] = ", ".join(stale)[:1000]
                self.log(f"  ! {name}: removed columns still mapped in lineage: {stale}")
        md = ExternalMetadata(
            name=name,
            system_type=system_type,
            entity_type=entity_type,
            columns=columns,
            description=facets.get("documentation", {}).get("description"),
            url=facets.get("demo_url", {}).get("url"),
            properties=props,
        )
        fingerprint = json.dumps([authoritative_schema, md.as_dict()], sort_keys=True)
        if self._seen_metadata.get(name) == fingerprint:
            return
        try:
            self.w.external_metadata.create_external_metadata(md)
            self.log(f"  + metadata  {name}  [{system_type.value}/{entity_type}]")
        except (AlreadyExists, ResourceAlreadyExists):
            if authoritative_schema:
                mask = ["columns", "properties"]
            else:
                # job events only fill descriptive fields; columns come from the schema watcher once it has seen data
                mask = [f for f in ("description", "url") if getattr(md, f)]
                current = self.w.external_metadata.get_external_metadata(name)
                if not current.columns and columns:
                    mask.append("columns")
            mask.append("entity_type")
            self.w.external_metadata.update_external_metadata(
                name=name, external_metadata=md, update_mask=",".join(mask))
            self.log(f"  ~ metadata  {name}" + (f"  schema v{change.get('version')}: "
                     f"+{change.get('added', [])} -{change.get('removed', [])} ~{change.get('type_changed', [])}"
                     if authoritative_schema and change else ""))
        self._seen_metadata[name] = fingerprint

    def _stale_mappings(self, name: str, removed: set[str]) -> list[str]:
        """Column mappings on this object's edges that reference columns which no longer exist.
        UC does not validate mappings against metadata columns, so this is the only place drift surfaces."""
        me = ExternalLineageObject(external_metadata=ExternalLineageExternalMetadata(name=name))
        stale = []
        for direction in LineageDirection:
            for info in self.w.external_lineage.list_external_lineage_relationships(
                    object_info=me, lineage_direction=direction):
                other = _info_key(info)[1]
                for c in (info.external_lineage_info.columns or []):
                    mine = c.source if direction == LineageDirection.DOWNSTREAM else c.target
                    if mine in removed:
                        stale.append(f"{c.source}->{c.target} ({'to' if direction == LineageDirection.DOWNSTREAM else 'from'} {other})")
        return stale

    # ---------- relationships ----------
    def _existing_relationship_id(self, src: ExternalLineageObject, tgt: ExternalLineageObject) -> str | None:
        try:
            for info in self.w.external_lineage.list_external_lineage_relationships(
                object_info=src, lineage_direction=LineageDirection.DOWNSTREAM
            ):
                # the list API returns the *other* endpoint in a sibling field, not rel.target
                if info.external_lineage_info and _info_key(info) == _obj_key(tgt):
                    return info.external_lineage_info.id
        except NotFound:
            return None
        return None

    def upsert_relationship(self, src: Dataset, tgt: Dataset, columns: list[ColumnRelationship], props: dict) -> None:
        if src.is_databricks and tgt.is_databricks:
            return  # native UC lineage already covers this edge
        s_obj, t_obj = src.lineage_object(), tgt.lineage_object()
        if not src.is_databricks:
            s_obj.external_metadata.name = self._name(src)
        if not tgt.is_databricks:
            t_obj.external_metadata.name = self._name(tgt)
        label = f"{self._name(src)} -> {self._name(tgt)}"
        try:
            self.w.external_lineage.create_external_lineage_relationship(
                CreateRequestExternalLineage(source=s_obj, target=t_obj, columns=columns or None, properties=props)
            )
            self.log(f"  + lineage   {label}  ({len(columns)} cols)")
        except (AlreadyExists, ResourceAlreadyExists):
            rel_id = self._existing_relationship_id(s_obj, t_obj)
            self.w.external_lineage.update_external_lineage_relationship(
                external_lineage_relationship=UpdateRequestExternalLineage(
                    id=rel_id, source=s_obj, target=t_obj, columns=columns or None, properties=props
                ),
                update_mask="columns,properties",
            )
            self.log(f"  ~ lineage   {label}  ({len(columns)} cols)")

    # ---------- event entry point ----------
    def process_event(self, event: dict) -> int:
        """Apply one OpenLineage RunEvent or DatasetEvent. Returns number of edges written."""
        if "dataset" in event and "job" not in event:  # DatasetEvent: static metadata (e.g. schema change)
            d = event["dataset"]
            self.log(f"dataset-event {d['namespace']}/{d['name']}")
            self.upsert_metadata(Dataset(d["namespace"], d["name"], d.get("facets", {})), authoritative_schema=True)
            return 0
        # streaming jobs (Kafka Streams, Flink) announce lineage on START/RUNNING, batch jobs on COMPLETE
        if event.get("eventType", "COMPLETE") not in ("START", "RUNNING", "COMPLETE"):
            return 0
        job = event.get("job", {})
        run = event.get("run", {})
        inputs = [Dataset(d["namespace"], d["name"], d.get("facets", {})) for d in event.get("inputs", [])]
        outputs = [Dataset(d["namespace"], d["name"], d.get("facets", {})) for d in event.get("outputs", [])]
        self.log(f"event {job.get('namespace')}/{job.get('name')} run={run.get('runId', '')[:8]}")

        for ds in inputs + outputs:
            self.upsert_metadata(ds)

        props = {
            "openlineage_job": f"{job.get('namespace')}/{job.get('name')}",
            "openlineage_run_id": run.get("runId", ""),
            "event_time": event.get("eventTime", ""),
            "producer": event.get("producer", ""),
        }
        job_facets = job.get("facets", {})
        if job_facets.get("jobType", {}).get("processingType"):
            props["processing_type"] = job_facets["jobType"]["processingType"]
        if job_facets.get("documentation", {}).get("description"):
            props["transformation"] = job_facets["documentation"]["description"][:1000]
        edges = 0
        for out in outputs:
            col_facet = out.facets.get("columnLineage", {}).get("fields", {})
            for inp in inputs:
                cols = [
                    ColumnRelationship(source=f["field"], target=out_col)
                    for out_col, spec in col_facet.items()
                    for f in spec.get("inputFields", [])
                    if f["namespace"] == inp.namespace and f["name"] == inp.name
                ]
                self.upsert_relationship(inp, out, cols, props)
                edges += 1
        return edges


def _obj_key(o) -> tuple:
    if getattr(o, "table", None) and o.table:
        return ("table", o.table.name)
    if getattr(o, "external_metadata", None) and o.external_metadata:
        return ("ext", o.external_metadata.name)
    return ("other", str(o))


def _info_key(info) -> tuple:
    if info.table_info:
        return ("table", f"{info.table_info.catalog_name}.{info.table_info.schema_name}.{info.table_info.name}")
    if info.external_metadata_info:
        return ("ext", info.external_metadata_info.name)
    return ("other", None)


def info_to_object(info) -> ExternalLineageObject:
    """Turn the counterpart endpoint of a list result back into an ExternalLineageObject."""
    kind, name = _info_key(info)
    if kind == "table":
        return ExternalLineageObject(table=ExternalLineageTable(name=name))
    return ExternalLineageObject(external_metadata=ExternalLineageExternalMetadata(name=name))
