"""Background threads: spot dead datanodes, and copy chunks to restore missing replicas."""
import time

from common.chunk_client import fetch_chunk, store_chunk
from common.chunking import checksum_bytes
from common.config import HEARTBEAT_TIMEOUT_SEC, MONITOR_INTERVAL_SEC, RE_REPLICATION_INTERVAL_SEC
from common.utils import log


def run_health_monitor(store):
    while True:
        for dn_id in store.mark_dead_if_stale(HEARTBEAT_TIMEOUT_SEC):
            log("HEALTH", f"datanode '{dn_id}' marked DEAD (no heartbeat for {HEARTBEAT_TIMEOUT_SEC}s)")
        time.sleep(MONITOR_INTERVAL_SEC)


def run_re_replication(store):
    announced = 0
    while True:
        try:
            todo = store.under_replicated_chunks()
            if todo and len(todo) != announced:
                log("REPLICATE", f"{len(todo)} chunk(s) are missing a replica -- healing")
            announced = len(todo)
            for filename, chunk, src_id, dst_id in todo:
                _heal(store, filename, chunk, src_id, dst_id)
        except Exception as exc:
            log("REPLICATE", f"scan error: {exc}")
        time.sleep(RE_REPLICATION_INTERVAL_SEC)


def _heal(store, filename, chunk, src_id, dst_id):
    src, dst = store.get_datanode(src_id), store.get_datanode(dst_id)
    if not src or not dst:
        return
    try:
        data, checksum = fetch_chunk(src["host"], src["chunk_port"], chunk["chunk_id"])
        if checksum != chunk["checksum"] or checksum_bytes(data) != chunk["checksum"]:
            log("REPLICATE", f"{chunk['chunk_id']}: source copy on {src_id} is bad, skipping")
            return
        if store_chunk(dst["host"], dst["chunk_port"], chunk["chunk_id"], data, checksum):
            store.add_replica_location(filename, chunk["index"], dst_id)
            log("REPLICATE", f"copied {chunk['chunk_id']}: {src_id} -> {dst_id}")
    except Exception as exc:
        log("REPLICATE", f"could not heal {chunk['chunk_id']} ({src_id} -> {dst_id}): {exc}")
