# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import logging
import os.path
import threading
from typing import Any
from typing import Dict
from typing import List
from typing import TYPE_CHECKING

import tinydb

if TYPE_CHECKING:
    import tinydb.table


class SerializeAccessToDB:
    crit = threading.RLock()
    locks: Dict[str, threading.RLock] = {}


class Database:
    def __init__(self, db_path: str, db_label: str = "db"):
        self._db_path = os.path.abspath(os.path.normpath(db_path))
        self._db: tinydb.TinyDB | None = None
        self._db_label = db_label
        self._log = logging.getLogger(db_label)

        with SerializeAccessToDB.crit:
            if self._db_path not in SerializeAccessToDB.locks:
                SerializeAccessToDB.locks[self._db_path] = threading.RLock()
            self._lock = SerializeAccessToDB.locks[self._db_path]

    def __enter__(self):
        self._lock.acquire()

        try:
            parent_dir = os.path.dirname(self._db_path)

            if os.path.exists(parent_dir) is False:
                os.makedirs(parent_dir, exist_ok=True)
            self._db = tinydb.TinyDB(self._db_path)
        except Exception:
            self._lock.release()
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._db.close()
        except Exception:
            raise
        finally:
            self._lock.release()

    def query(self, ql: tinydb.table.QueryLike | None = None) -> List[tinydb.table.Document]:
        if ql:
            return self._db.search(cond=ql)
        return self._db.all()

    def delete(self, ql: tinydb.table.QueryLike) -> int:
        """Delete documents
        Returns how many documents it deleted"""

        x = len(self._db.remove(cond=ql))
        self._log.info(f"Deleted {x} for query {ql}")
        return x

    def insert_many(self, docs: List[tinydb.table.Document | Dict[str, Any]]):
        for doc in docs:
            self._db.insert(doc)

    def upsert(self, doc: tinydb.table.Document | Dict[str, Any], ql: tinydb.table.QueryLike):
        self._db.upsert(doc, ql)
