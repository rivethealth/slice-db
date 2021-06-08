# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["test_schema 1"] = {
    "references": {
        "public.child.child_parent_id_fkey": {
            "columns": ["parent_id"],
            "referenceColumns": ["id"],
            "referenceTable": "public.parent",
            "table": "public.child",
        }
    },
    "sequences": {
        "public.child_id_seq": {"name": "child_id_seq", "schema": "public"},
        "public.parent_id_seq": {"name": "parent_id_seq", "schema": "public"},
    },
    "tables": {
        "public.child": {
            "columns": ["id", "parent_id"],
            "name": "child",
            "schema": "public",
            "sequences": ["public.child_id_seq"],
        },
        "public.parent": {
            "columns": ["id"],
            "name": "parent",
            "schema": "public",
            "sequences": ["public.parent_id_seq"],
        },
    },
}
