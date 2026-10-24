"""TCP listener that receives heartbeats from datanodes."""
import socket
import threading

from common.protocol import recv_msg, send_msg
from common.utils import log


def _handle(conn, addr, store):
    try:
        conn.settimeout(5)
        header, _ = recv_msg(conn)
        if header.get("op") != "HEARTBEAT":
            send_msg(conn, {"status": "ERROR", "reason": "expected HEARTBEAT"})
            return
        is_new, was_dead = store.register_heartbeat(
            header["datanode_id"], header["host"], header["chunk_port"],
            header.get("chunk_count", 0), header.get("storage_bytes", 0))
        if is_new:
            log("HEARTBEAT", f"datanode '{header['datanode_id']}' joined "
                             f"({header['host']}:{header['chunk_port']})")
        elif was_dead:
            log("HEARTBEAT", f"datanode '{header['datanode_id']}' is back ALIVE")
        send_msg(conn, {"status": "OK"})
    except Exception as exc:
        log("HEARTBEAT", f"bad heartbeat from {addr}: {exc}")
    finally:
        conn.close()


def run_heartbeat_server(host, port, store):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(16)
    log("HEARTBEAT", f"listening for heartbeats on {host}:{port}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=_handle, args=(conn, addr, store), daemon=True).start()
