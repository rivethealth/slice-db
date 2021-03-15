# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["test_schema 1"] = {
    "references": [
        {
            "columns": ["parent_id"],
            "id": "public.child.child_parent_id_fkey",
            "referenceColumns": ["id"],
            "referenceTable": "public.parent",
            "table": "public.child",
        }
    ],
    "tables": [
        {
            "columns": ["id"],
            "id": "public.parent",
            "name": "parent",
            "schema": "public",
        },
        {
            "columns": ["id", "parent_id"],
            "id": "public.child",
            "name": "child",
            "schema": "public",
        },
    ],
}
