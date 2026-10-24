"""
End-to-end check from the command line: upload a file, download it back,
compare SHA-256, then read every replica and check its checksum.

  python scripts/verify_roundtrip.py --size-mb 7
  python scripts/verify_roundtrip.py --file path\to\anything.pdf
"""
import argparse
import hashlib
import os
import sys

import requests


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--client", default="http://127.0.0.1:8000")
    p.add_argument("--file")
    p.add_argument("--size-mb", type=float, help="generate a random file of this size instead")
    a = p.parse_args()

    if a.file:
        name, data = os.path.basename(a.file), open(a.file, "rb").read()
    elif a.size_mb:
        name, data = f"random_{a.size_mb:g}mb.bin", os.urandom(int(a.size_mb * 1024 * 1024))
    else:
        sys.exit("give --file or --size-mb")

    print(f"file: {name}  size: {len(data)} bytes  sha256: {sha(data)[:16]}...")
    r = requests.post(f"{a.client}/api/upload", files={"file": (name, data)})
    print("upload:", r.status_code, r.json())
    if r.status_code != 200:
        sys.exit(1)
    stored = r.json()["filename"]

    d = requests.get(f"{a.client}/api/download/{stored}")
    ok = d.status_code == 200 and sha(d.content) == sha(data)
    print(f"download: {d.status_code}  size: {len(d.content)} bytes  sha256: {sha(d.content)[:16]}...")
    print("ROUND TRIP:", "MATCH (byte-for-byte identical)" if ok else "MISMATCH")

    v = requests.get(f"{a.client}/api/verify/{stored}").json()
    print(f"replica check: {v['replicas_ok']}/{v['replicas_checked']} copies match their checksum")
    sys.exit(0 if ok and v["all_ok"] else 1)


if __name__ == "__main__":
    main()
