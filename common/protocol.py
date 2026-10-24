"""
Tiny message format used on every TCP connection between nodes.

  [4 bytes: header length][header as JSON][payload bytes, if any]

The header always says how long the payload is ("payload_size"), so the
receiver knows exactly how many bytes to read.
"""
import json
import struct


def send_msg(sock, header, payload=b""):
    header = dict(header)
    header["payload_size"] = len(payload)
    raw = json.dumps(header).encode("utf-8")
    sock.sendall(struct.pack(">I", len(raw)))
    sock.sendall(raw)
    if payload:
        sock.sendall(payload)


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(min(65536, n - len(buf)))
        if not part:
            raise ConnectionError("connection closed before all bytes arrived")
        buf.extend(part)
    return bytes(buf)


def recv_msg(sock):
    header_len = struct.unpack(">I", _recv_exact(sock, 4))[0]
    header = json.loads(_recv_exact(sock, header_len).decode("utf-8"))
    size = header.get("payload_size", 0)
    payload = _recv_exact(sock, size) if size else b""
    return header, payload
