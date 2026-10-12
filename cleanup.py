"""Delete every extlin__ external metadata object (and its lineage relationships).

  python3.11 cleanup.py [profile]
"""
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    DeleteRequestExternalLineage, ExternalLineageExternalMetadata, ExternalLineageObject, LineageDirection,
)

from lineage_bridge.bridge import info_to_object

PREFIX = "extlin__"
w = WorkspaceClient(profile=sys.argv[1] if len(sys.argv) > 1 else "DEFAULT")
for md in w.external_metadata.list_external_metadata():
    if not md.name.startswith(PREFIX):
        continue
    me = ExternalLineageObject(external_metadata=ExternalLineageExternalMetadata(name=md.name))
    for direction in LineageDirection:
        for info in list(w.external_lineage.list_external_lineage_relationships(object_info=me, lineage_direction=direction)):
            other = info_to_object(info)
            src, tgt = (other, me) if direction == LineageDirection.UPSTREAM else (me, other)
            try:
                w.external_lineage.delete_external_lineage_relationship(
                    external_lineage_relationship=DeleteRequestExternalLineage(
                        source=src, target=tgt, id=info.external_lineage_info.id))
            except NotFound:
                pass  # already removed via the other endpoint
    w.external_metadata.delete_external_metadata(md.name)
    print(f"deleted {md.name}")
