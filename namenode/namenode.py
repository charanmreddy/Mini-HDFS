"""
Namenode: the controller. It never stores file bytes, only metadata.

  * HTTP API (Flask)   - used by the client app: allocate, commit, plan, status
  * TCP heartbeat port - datanodes report in here
  * background threads - dead-node detection and re-replication
"""
import argparse
import threading

from flask import Flask, jsonify, request

from common.chunk_client import delete_chunk
from common.utils import log
from namenode.health_monitor import run_health_monitor, run_re_replication
from namenode.heartbeat_server import run_heartbeat_server
from namenode.metadata_store import MetadataStore


def create_app(store):
    app = Flask(__name__)

    @app.get("/status")
    def status():
        return jsonify(store.snapshot_status())

    @app.get("/files")
    def files():
        return jsonify(store.list_files())

    @app.post("/allocate/<filename>")
    def allocate(filename):
        size = int((request.get_json(force=True, silent=True) or {}).get("size", 0))
        try:
            plan = store.allocate_file(filename, size)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        log("NAMENODE", f"allocated '{filename}': {size} bytes -> {len(plan)} chunk(s)")
        return jsonify({"chunks": plan})

    @app.post("/commit/<filename>")
    def commit(filename):
        confirmed = (request.get_json(force=True, silent=True) or {}).get("chunks", [])
        result = store.commit_file(filename, confirmed)
        log("NAMENODE", f"commit '{filename}': {result.get('status')}")
        return jsonify(result)

    @app.get("/plan/<filename>")
    def plan(filename):
        p = store.get_download_plan(filename)
        return (jsonify(p), 200) if p else (jsonify({"error": "file not found"}), 404)

    @app.get("/chunkmap/<filename>")
    def chunkmap(filename):
        m = store.chunk_map(filename)
        return (jsonify(m), 200) if m else (jsonify({"error": "file not found"}), 404)

    @app.delete("/files/<filename>")
    def delete(filename):
        chunks = store.delete_file(filename)
        if chunks is None:
            return jsonify({"error": "file not found"}), 404
        for c in chunks:                      # best effort cleanup on datanodes
            for dn_id in c["locations"]:
                dn = store.get_datanode(dn_id)
                if dn and dn["alive"]:
                    try:
                        delete_chunk(dn["host"], dn["chunk_port"], c["chunk_id"])
                    except Exception:
                        pass
        log("NAMENODE", f"deleted '{filename}'")
        return jsonify({"status": "deleted"})

    return app


def main():
    p = argparse.ArgumentParser(description="Mini-HDFS namenode")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=9000, help="HTTP API port")
    p.add_argument("--heartbeat-port", type=int, default=9100, help="TCP port for datanode heartbeats")
    p.add_argument("--image", default="namenode_data/fsimage.json", help="where metadata is saved")
    a = p.parse_args()

    store = MetadataStore(a.image)
    threading.Thread(target=run_heartbeat_server, args=(a.host, a.heartbeat_port, store), daemon=True).start()
    threading.Thread(target=run_health_monitor, args=(store,), daemon=True).start()
    threading.Thread(target=run_re_replication, args=(store,), daemon=True).start()
    log("NAMENODE", f"HTTP API on {a.host}:{a.port}, heartbeats on {a.host}:{a.heartbeat_port}")
    create_app(store).run(host=a.host, port=a.port, threaded=True)


if __name__ == "__main__":
    main()
