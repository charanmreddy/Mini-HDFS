"""
Datanode: stores chunks and tells the namenode it is alive.

Two threads:
  * chunk server    - answers STORE / GET / DELETE requests over TCP
  * heartbeat sender - tells the namenode "still here" every few seconds
"""
import argparse
import socket
import threading
import time

from common.config import HEARTBEAT_INTERVAL_SEC
from common.chunking import checksum_bytes
from common.protocol import recv_msg, send_msg
from common.utils import log
from datanode.storage import ChunkStorage, CorruptChunk


def handle(conn, addr, storage, dn_id):
    try:
        conn.settimeout(30)
        header, payload = recv_msg(conn)
        op, chunk_id = header.get("op"), header.get("chunk_id")

        if op == "STORE":
            actual = checksum_bytes(payload)
            if header.get("checksum") and actual != header["checksum"]:
                log(dn_id, f"REJECTED {chunk_id}: arrived corrupted (checksum mismatch)")
                send_msg(conn, {"status": "ERROR", "reason": "checksum mismatch on receive"})
                return
            storage.save(chunk_id, payload, actual)
            log(dn_id, f"stored {chunk_id} ({len(payload)} bytes)")
            send_msg(conn, {"status": "OK"})

        elif op == "GET":
            try:
                result = storage.load(chunk_id)
            except CorruptChunk as exc:
                log(dn_id, f"CORRUPTION DETECTED on read: {exc}")
                send_msg(conn, {"status": "ERROR", "reason": "chunk is corrupted on disk"})
                return
            if result is None:
                log(dn_id, f"chunk not found: {chunk_id}")
                send_msg(conn, {"status": "ERROR", "reason": "chunk not found"})
                return
            data, checksum = result
            send_msg(conn, {"status": "OK", "checksum": checksum}, data)
            log(dn_id, f"served {chunk_id} ({len(data)} bytes)")

        elif op == "DELETE":
            storage.delete(chunk_id)
            log(dn_id, f"deleted {chunk_id}")
            send_msg(conn, {"status": "OK"})

        else:
            send_msg(conn, {"status": "ERROR", "reason": f"unknown op {op}"})
    except Exception as exc:
        log(dn_id, f"error handling {addr}: {exc}")
    finally:
        conn.close()


def chunk_server(host, port, storage, dn_id):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(32)
    log(dn_id, f"chunk service listening on {host}:{port}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, addr, storage, dn_id), daemon=True).start()


def heartbeat_sender(nn_host, nn_port, dn_id, self_host, port, storage):
    was_failing = False
    while True:
        try:
            with socket.create_connection((nn_host, nn_port), timeout=5) as sock:
                send_msg(sock, {"op": "HEARTBEAT", "datanode_id": dn_id, "host": self_host,
                                "chunk_port": port, "chunk_count": storage.count(),
                                "storage_bytes": storage.total_bytes()})
                header, _ = recv_msg(sock)
                if was_failing:
                    log(dn_id, "heartbeat to namenode restored")
                    was_failing = False
                if header.get("status") != "OK":
                    log(dn_id, f"namenode rejected heartbeat: {header}")
        except Exception as exc:
            if not was_failing:
                log(dn_id, f"heartbeat failed: {exc}")
            was_failing = True
        time.sleep(HEARTBEAT_INTERVAL_SEC)


def main():
    p = argparse.ArgumentParser(description="Mini-HDFS datanode")
    p.add_argument("--id", required=True, help="unique name, e.g. dn0")
    p.add_argument("--host", default="0.0.0.0", help="interface to listen on")
    p.add_argument("--port", type=int, required=True, help="TCP port for chunk traffic")
    p.add_argument("--self-host", required=True, help="address the other nodes use to reach this one")
    p.add_argument("--storage-dir", default=None)
    p.add_argument("--namenode-host", required=True)
    p.add_argument("--namenode-heartbeat-port", type=int, default=9100)
    a = p.parse_args()

    storage = ChunkStorage(a.storage_dir or f"data/{a.id}")
    log(a.id, f"storing chunks in {storage.dir} ({storage.count()} already on disk)")
    threading.Thread(target=chunk_server, args=(a.host, a.port, storage, a.id), daemon=True).start()
    threading.Thread(target=heartbeat_sender, daemon=True,
                     args=(a.namenode_host, a.namenode_heartbeat_port, a.id, a.self_host, a.port, storage)).start()
    log(a.id, "datanode is up")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
