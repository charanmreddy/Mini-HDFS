"""Print the cluster state as plain text (handy for copying into a report)."""
import argparse

import requests


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--namenode", default="http://127.0.0.1:9000")
    a = p.parse_args()
    s = requests.get(a.namenode + "/status", timeout=5).json()
    print("DATANODES")
    for d in sorted(s["datanodes"].values(), key=lambda d: d["id"]):
        print(f"  {d['id']:<5} {'ALIVE' if d['alive'] else 'DEAD ':<5} chunks={d['chunk_count']:<3} "
              f"bytes={d['storage_bytes']:<9} last_heartbeat={d['age_sec']}s ago")
    print(f"FILES ({s['total_files']}), CHUNKS ({s['total_chunks']}), UNDER-REPLICATED ({s['under_replicated']})")
    for n, f in sorted(s["files"].items()):
        print(f"  {n:<24} {f['status']:<9} size={f['size']:<9} chunks={f['num_chunks']:<3} short={f['under_replicated']}")


if __name__ == "__main__":
    main()
