"""Talk to a datanode's TCP chunk service. Used by the client app and the namenode."""
import socket

from common.config import SOCKET_TIMEOUT_SEC
from common.protocol import recv_msg, send_msg


def store_chunk(host, port, chunk_id, data, checksum):
    with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT_SEC) as sock:
        send_msg(sock, {"op": "STORE", "chunk_id": chunk_id, "checksum": checksum}, data)
        header, _ = recv_msg(sock)
        return header.get("status") == "OK"


def fetch_chunk(host, port, chunk_id):
    """Returns (data, checksum_reported_by_datanode). Raises if the datanode refuses."""
    with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT_SEC) as sock:
        send_msg(sock, {"op": "GET", "chunk_id": chunk_id})
        header, payload = recv_msg(sock)
        if header.get("status") != "OK":
            raise RuntimeError(header.get("reason", "datanode returned an error"))
        return payload, header.get("checksum")


def delete_chunk(host, port, chunk_id):
    with socket.create_connection((host, port), timeout=SOCKET_TIMEOUT_SEC) as sock:
        send_msg(sock, {"op": "DELETE", "chunk_id": chunk_id})
        header, _ = recv_msg(sock)
        return header.get("status") == "OK"
