"""
Client app (runs on the "client" machine).

  * serves the dashboard page
  * upload: asks the namenode for a plan, cuts the file into 2 MB chunks,
            pushes each chunk straight to the datanodes over TCP, then commits
  * download: asks the namenode where chunks live, pulls them straight from
              the datanodes over TCP, checks every checksum, rebuilds the file
"""
import argparse
import io
import os
import tempfile
from urllib.parse import quote

import requests
from flask import Flask, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename

from common.chunk_client import fetch_chunk, store_chunk
from common.chunking import checksum_bytes
from common.utils import log

NAMENODE = "http://127.0.0.1:9000"


def nn(method, path, **kw):
    return requests.request(method, NAMENODE + path, timeout=10, **kw)


def create_app():
    app = Flask(__name__, static_folder="static", template_folder="templates")

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/api/status")
    def api_status():
        try:
            r = nn("GET", "/status")
            return jsonify(r.json()), r.status_code
        except requests.RequestException as exc:
            return jsonify({"error": f"namenode unreachable: {exc}"}), 502

    @app.get("/api/chunkmap/<filename>")
    def api_chunkmap(filename):
        try:
            r = nn("GET", f"/chunkmap/{quote(filename)}")
            return jsonify(r.json()), r.status_code
        except requests.RequestException as exc:
            return jsonify({"error": f"namenode unreachable: {exc}"}), 502

    @app.post("/api/upload")
    def api_upload():
        f = request.files.get("file")
        name = secure_filename(f.filename) if f and f.filename else ""
        if not name:
            return jsonify({"error": "no usable file name"}), 400

        fd, tmp = tempfile.mkstemp()
        os.close(fd)                     # close first: Windows can't reopen an open temp file
        try:
            f.save(tmp)
            size = os.path.getsize(tmp)
            try:
                r = nn("POST", f"/allocate/{quote(name)}", json={"size": size})
            except requests.RequestException as exc:
                return jsonify({"error": f"namenode unreachable: {exc}"}), 502
            if r.status_code != 200:
                return jsonify({"error": r.json().get("error", "allocate failed")}), 400
            plan = r.json()["chunks"]

            confirmed, warnings = [], []
            with open(tmp, "rb") as fh:
                for entry in plan:
                    fh.seek(entry["offset"])
                    data = fh.read(entry["length"])
                    checksum = checksum_bytes(data)
                    ok_nodes = []
                    for t in entry["targets"]:
                        try:
                            if store_chunk(t["host"], t["chunk_port"], entry["chunk_id"], data, checksum):
                                ok_nodes.append(t["datanode_id"])
                            else:
                                warnings.append(f"{t['datanode_id']} rejected chunk {entry['index']}")
                        except Exception as exc:
                            warnings.append(f"{t['datanode_id']} unreachable for chunk {entry['index']}")
                            log("CLIENT", f"could not store {entry['chunk_id']} on {t['datanode_id']}: {exc}")
                    confirmed.append({"index": entry["index"], "checksum": checksum, "locations": ok_nodes})

            result = nn("POST", f"/commit/{quote(name)}", json={"chunks": confirmed}).json()
            copies = sum(len(c["locations"]) for c in confirmed)
            log("CLIENT", f"uploaded '{name}': {size} bytes, {len(plan)} chunks, {copies} copies -> {result['status']}")
            return jsonify({"filename": name, "size": size, "status": result["status"],
                            "chunks": len(plan), "copies_stored": copies, "warnings": warnings}), \
                (200 if result["status"] == "COMPLETE" else 502)
        finally:
            os.remove(tmp)

    def _rebuild(filename):
        """Returns (bytes, None) or (None, error_message)."""
        r = nn("GET", f"/plan/{quote(filename)}")
        if r.status_code != 200:
            return None, "file not found"
        plan = r.json()
        if plan["status"] != "COMPLETE":
            return None, f"file status is {plan['status']}"
        buf = io.BytesIO()
        for c in plan["chunks"]:
            data, last = None, "no live replica"
            for loc in c["locations"]:
                try:
                    payload, cks = fetch_chunk(loc["host"], loc["chunk_port"], c["chunk_id"])
                    if cks == c["checksum"] and checksum_bytes(payload) == c["checksum"]:
                        data = payload
                        break
                    last = f"bad checksum from {loc['datanode_id']}"
                except Exception as exc:
                    last = f"{loc['datanode_id']}: {exc}"
                log("CLIENT", f"chunk {c['index']}: {last} -- trying next replica")
            if data is None:
                return None, f"chunk {c['index']} unavailable ({last})"
            buf.write(data)
        buf.seek(0)
        return buf, None

    @app.get("/api/download/<filename>")
    def api_download(filename):
        try:
            buf, err = _rebuild(filename)
        except requests.RequestException as exc:
            return jsonify({"error": f"namenode unreachable: {exc}"}), 502
        if err:
            return jsonify({"error": err}), 404 if err == "file not found" else 502
        log("CLIENT", f"rebuilt '{filename}' ({buf.getbuffer().nbytes} bytes)")
        return send_file(buf, as_attachment=True, download_name=filename)

    @app.get("/api/verify/<filename>")
    def api_verify(filename):
        """Fetch EVERY replica of EVERY chunk and compare it with the recorded checksum."""
        r = nn("GET", f"/plan/{quote(filename)}")
        if r.status_code != 200:
            return jsonify({"error": "file not found"}), 404
        out, total, good = [], 0, 0
        for c in r.json()["chunks"]:
            reps = []
            for loc in c["locations"]:
                total += 1
                try:
                    payload, cks = fetch_chunk(loc["host"], loc["chunk_port"], c["chunk_id"])
                    ok = cks == c["checksum"] and checksum_bytes(payload) == c["checksum"]
                    res = "ok" if ok else "corrupt"
                except RuntimeError as exc:
                    res = "corrupt" if "corrupt" in str(exc) else "missing"
                except Exception:
                    res = "unreachable"
                good += res == "ok"
                reps.append({"datanode": loc["datanode_id"], "result": res})
            out.append({"index": c["index"], "replicas": reps})
        log("CLIENT", f"verify '{filename}': {good}/{total} replicas good")
        return jsonify({"filename": filename, "chunks": out, "replicas_checked": total,
                        "replicas_ok": good, "all_ok": total > 0 and good == total})

    @app.delete("/api/files/<filename>")
    def api_delete(filename):
        r = nn("DELETE", f"/files/{quote(filename)}")
        return jsonify(r.json()), r.status_code

    return app


def main():
    global NAMENODE
    p = argparse.ArgumentParser(description="Mini-HDFS client + dashboard")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--namenode-url", required=True, help="e.g. http://127.0.0.1:9000")
    a = p.parse_args()
    NAMENODE = a.namenode_url.rstrip("/")
    log("CLIENT", f"namenode is at {NAMENODE}; dashboard on http://127.0.0.1:{a.port}")
    create_app().run(host=a.host, port=a.port, threaded=True)


if __name__ == "__main__":
    main()
