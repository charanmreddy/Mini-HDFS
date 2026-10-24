"""Chunk files on disk. Each chunk has a sidecar file holding its SHA-256."""
import hashlib
import os
import re
import threading


class CorruptChunk(Exception):
    pass


class ChunkStorage:
    def __init__(self, storage_dir):
        self.dir = storage_dir
        os.makedirs(self.dir, exist_ok=True)
        self.lock = threading.Lock()

    def _base(self, chunk_id):
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", chunk_id)
        tag = hashlib.sha1(chunk_id.encode()).hexdigest()[:8]
        return os.path.join(self.dir, f"{safe}_{tag}")

    def save(self, chunk_id, data, checksum):
        base = self._base(chunk_id)
        with self.lock:
            with open(base + ".tmp", "wb") as f:
                f.write(data)
            os.replace(base + ".tmp", base + ".chunk")
            with open(base + ".sha256", "w") as f:
                f.write(checksum)

    def load(self, chunk_id):
        """Returns (data, checksum) or None if missing. Raises CorruptChunk if the bytes changed."""
        base = self._base(chunk_id)
        if not os.path.exists(base + ".chunk") or not os.path.exists(base + ".sha256"):
            return None
        with open(base + ".chunk", "rb") as f:
            data = f.read()
        with open(base + ".sha256") as f:
            expected = f.read().strip()
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            raise CorruptChunk(f"{chunk_id}: stored checksum {expected[:10]} != actual {actual[:10]}")
        return data, actual

    def delete(self, chunk_id):
        base = self._base(chunk_id)
        for ext in (".chunk", ".sha256", ".tmp"):
            if os.path.exists(base + ext):
                os.remove(base + ext)

    def count(self):
        return len([n for n in os.listdir(self.dir) if n.endswith(".chunk")])

    def total_bytes(self):
        return sum(os.path.getsize(os.path.join(self.dir, n))
                   for n in os.listdir(self.dir) if n.endswith(".chunk"))
