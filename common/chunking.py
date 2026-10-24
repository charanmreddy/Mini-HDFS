import hashlib

from common.config import CHUNK_SIZE


def checksum_bytes(data):
    return hashlib.sha256(data).hexdigest()


def make_chunk_id(filename, index):
    return f"{filename}.chunk{index:05d}"


def plan_chunks(total_size, chunk_size=CHUNK_SIZE):
    """Split logic: returns [(index, offset, length), ...] covering the file."""
    plan, offset, index = [], 0, 0
    while offset < total_size:
        length = min(chunk_size, total_size - offset)
        plan.append((index, offset, length))
        offset += length
        index += 1
    return plan
