"""
The namenode's memory: which datanodes exist, which files exist, and where
every chunk lives. File metadata is also saved to disk (fsimage.json) so it
survives a namenode restart. Datanode info is rebuilt from heartbeats.
"""
import json
import os
import threading
import time

from common.chunking import make_chunk_id, plan_chunks
from common.config import REPLICATION_FACTOR
from common.utils import log


class MetadataStore:
    def __init__(self, image_path="namenode_data/fsimage.json"):
        self.lock = threading.RLock()
        self.datanodes = {}   # id -> info dict
        self.files = {}       # filename -> {status,size,num_chunks,created_at,chunks:[...]}
        self._rr = 0
        self.image_path = image_path
        self._load()

    # ---------- persistence ----------
    def _load(self):
        if os.path.exists(self.image_path):
            with open(self.image_path) as f:
                self.files = json.load(f)
            log("NAMENODE", f"loaded metadata for {len(self.files)} file(s) from {self.image_path}")

    def _persist(self):
        os.makedirs(os.path.dirname(self.image_path) or ".", exist_ok=True)
        tmp = self.image_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.files, f)
        os.replace(tmp, self.image_path)

    # ---------- datanode registry ----------
    def register_heartbeat(self, dn_id, host, chunk_port, chunk_count, storage_bytes):
        with self.lock:
            old = self.datanodes.get(dn_id)
            self.datanodes[dn_id] = {"id": dn_id, "host": host, "chunk_port": chunk_port,
                                     "last_heartbeat": time.time(), "alive": True,
                                     "chunk_count": chunk_count, "storage_bytes": storage_bytes}
            return old is None, (old is not None and not old["alive"])

    def mark_dead_if_stale(self, timeout):
        dead = []
        with self.lock:
            now = time.time()
            for dn_id, dn in self.datanodes.items():
                if dn["alive"] and now - dn["last_heartbeat"] > timeout:
                    dn["alive"] = False
                    dead.append(dn_id)
        return dead

    def get_datanode(self, dn_id):
        with self.lock:
            dn = self.datanodes.get(dn_id)
            return dict(dn) if dn else None

    def alive_ids(self):
        with self.lock:
            return sorted(i for i, d in self.datanodes.items() if d["alive"])

    def _addr(self, dn_id):
        dn = self.datanodes[dn_id]
        return {"datanode_id": dn_id, "host": dn["host"], "chunk_port": dn["chunk_port"]}

    def _pick_targets(self, rf):
        alive = self.alive_ids()
        if not alive:
            raise ValueError("no datanodes are alive")
        n = min(rf, len(alive))
        start = self._rr % len(alive)       # rotate so the "first" replica alternates
        self._rr += 1
        return [alive[(start + i) % len(alive)] for i in range(n)]

    # ---------- upload ----------
    def allocate_file(self, filename, size):
        """Split logic + placement. Returns the plan the client must follow."""
        if size <= 0:
            raise ValueError("file is empty")
        with self.lock:
            chunks, plan = [], []
            for index, offset, length in plan_chunks(size):
                cid = make_chunk_id(filename, index)
                targets = self._pick_targets(REPLICATION_FACTOR)
                chunks.append({"chunk_id": cid, "index": index, "size": length,
                               "checksum": None, "locations": []})
                plan.append({"index": index, "chunk_id": cid, "offset": offset, "length": length,
                             "targets": [self._addr(t) for t in targets]})
            self.files[filename] = {"status": "PENDING", "size": size, "num_chunks": len(chunks),
                                    "created_at": time.time(), "chunks": chunks}
            return plan

    def commit_file(self, filename, confirmed):
        with self.lock:
            meta = self.files.get(filename)
            if meta is None:
                return {"status": "ERROR", "reason": "unknown file; call allocate first"}
            by_index = {c["index"]: c for c in confirmed}
            ok = True
            for chunk in meta["chunks"]:
                c = by_index.get(chunk["index"], {})
                chunk["checksum"] = c.get("checksum")
                chunk["locations"] = c.get("locations", [])
                if not chunk["locations"] or not chunk["checksum"]:
                    ok = False
            meta["status"] = "COMPLETE" if ok else "FAILED"
            self._persist()
            return {"status": meta["status"], "num_chunks": meta["num_chunks"]}

    # ---------- download / dashboard ----------
    def get_download_plan(self, filename):
        with self.lock:
            meta = self.files.get(filename)
            if meta is None:
                return None
            alive = set(self.alive_ids())
            chunks = []
            for c in sorted(meta["chunks"], key=lambda c: c["index"]):
                chunks.append({"index": c["index"], "chunk_id": c["chunk_id"], "size": c["size"],
                               "checksum": c["checksum"],
                               "locations": [self._addr(l) for l in c["locations"] if l in alive]})
            return {"filename": filename, "status": meta["status"], "size": meta["size"],
                    "num_chunks": meta["num_chunks"], "chunks": chunks}

    def chunk_map(self, filename):
        with self.lock:
            meta = self.files.get(filename)
            if meta is None:
                return None
            alive = set(self.alive_ids())
            known = sorted(self.datanodes)
            rows = []
            for c in sorted(meta["chunks"], key=lambda c: c["index"]):
                rows.append({"index": c["index"], "chunk_id": c["chunk_id"], "size": c["size"],
                             "checksum": c["checksum"],
                             "replicas": {d: ("alive" if d in alive else "dead")
                                          for d in c["locations"]}})
            return {"filename": filename, "status": meta["status"], "size": meta["size"],
                    "datanodes": known, "chunks": rows}

    def _live_count(self, chunk, alive):
        return len([l for l in chunk["locations"] if l in alive])

    def list_files(self):
        with self.lock:
            alive = set(self.alive_ids())
            out = {}
            for name, m in self.files.items():
                under = sum(1 for c in m["chunks"]
                            if m["status"] == "COMPLETE" and self._live_count(c, alive) < REPLICATION_FACTOR)
                out[name] = {"status": m["status"], "size": m["size"], "num_chunks": m["num_chunks"],
                             "created_at": m["created_at"], "under_replicated": under}
            return out

    def snapshot_status(self):
        with self.lock:
            now = time.time()
            files = self.list_files()
            return {"datanodes": {i: {**d, "age_sec": round(now - d["last_heartbeat"], 1)}
                                  for i, d in self.datanodes.items()},
                    "files": files,
                    "total_files": len(files),
                    "total_chunks": sum(f["num_chunks"] for f in files.values()),
                    "under_replicated": sum(f["under_replicated"] for f in files.values()),
                    "replication_factor": REPLICATION_FACTOR}

    def delete_file(self, filename):
        """Returns the file's chunk list (for cleanup on datanodes) or None."""
        with self.lock:
            meta = self.files.pop(filename, None)
            if meta is None:
                return None
            self._persist()
            return meta["chunks"]

    # ---------- re-replication ----------
    def under_replicated_chunks(self):
        """[(filename, chunk, source_id, target_id)] for every chunk missing a live copy."""
        out = []
        with self.lock:
            alive = set(self.alive_ids())
            for name, m in self.files.items():
                if m["status"] != "COMPLETE":
                    continue
                for c in m["chunks"]:
                    live = [l for l in c["locations"] if l in alive]
                    if not live or len(live) >= REPLICATION_FACTOR:
                        continue
                    targets = [d for d in alive if d not in live]
                    if targets:
                        out.append((name, dict(c), live[0], targets[0]))
        return out

    def add_replica_location(self, filename, index, dn_id):
        with self.lock:
            m = self.files.get(filename)
            if not m:
                return
            for c in m["chunks"]:
                if c["index"] == index and dn_id not in c["locations"]:
                    c["locations"].append(dn_id)
            self._persist()
