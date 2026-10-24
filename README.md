# Mini HDFS

A small simulation of HDFS in Python. Files are split into 2 MB chunks, every chunk is stored on two datanodes, and a namenode keeps track of where everything is.

I built it to understand how chunking, replication and heartbeats fit together. It is a learning project, not something to use for real data.

## How it works

- **Namenode**: decides how a file is split and which datanodes get each chunk. It only keeps metadata (saved to `namenode_data/fsimage.json`), never the file itself.
- **Datanodes**: store chunks on disk, each with a SHA-256 checksum file next to it. They send a heartbeat to the namenode every 2 seconds. If the namenode hears nothing for 6 seconds, it marks that node dead.
- **Client**: a Flask app with a web dashboard. It sends chunks straight to the datanodes over TCP and reads them back the same way. The namenode just tells it where to go.
- **Recovery**: when a dead datanode comes back, the namenode copies over any chunks it is missing.
- **Integrity**: chunks are checked on write, on read, and when the file is put back together. If a copy is corrupted, the client skips it and uses the other one.

## Folders

```
common/     shared code: message format, chunk splitting, TCP helpers, settings
namenode/   namenode.py, metadata_store.py, heartbeat_server.py, health_monitor.py
datanode/   datanode.py, storage.py
client/     app.py, templates/, static/ (the dashboard)
scripts/    verify_roundtrip.py, show_status.py
```

## Setup

Python 3.9 or newer. Only needs Flask and requests.

```
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

On Linux or macOS, activate with `source .venv/bin/activate`.

## Running

Run each of these in its own terminal, from the project folder:

```
python -m namenode.namenode --port 9000 --heartbeat-port 9100
python -m datanode.datanode --id dn0 --port 9201 --self-host 127.0.0.1 --namenode-host 127.0.0.1 --storage-dir data/dn0
python -m datanode.datanode --id dn1 --port 9202 --self-host 127.0.0.1 --namenode-host 127.0.0.1 --storage-dir data/dn1
python -m client.app --port 8000 --namenode-url http://127.0.0.1:9000
```

Then open http://127.0.0.1:8000.

To use separate machines, run each command on its own machine and replace `127.0.0.1` with the namenode's IP. For datanodes, `--self-host` is that datanode's own IP.

## Trying it out

```
python scripts/verify_roundtrip.py --size-mb 7
python scripts/show_status.py
```

The first uploads a random file, downloads it, and compares the hashes. The second prints the state of the cluster.

To test failure, stop a datanode with Ctrl+C. It shows as dead on the dashboard after about 6 seconds, files still download, and uploads still work (with one copy). Start it again and the missing copies get rebuilt.
