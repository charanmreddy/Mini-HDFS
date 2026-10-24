"""Settings shared by every node. Change them here, not in the code."""

CHUNK_SIZE = 2 * 1024 * 1024        # 2 MB per chunk
REPLICATION_FACTOR = 2              # copies of every chunk (we have 2 datanodes)
HEARTBEAT_INTERVAL_SEC = 2          # datanode -> namenode "I'm alive" message
HEARTBEAT_TIMEOUT_SEC = 6           # no heartbeat for this long = datanode is dead
MONITOR_INTERVAL_SEC = 2            # how often the namenode checks for dead nodes
RE_REPLICATION_INTERVAL_SEC = 4     # how often the namenode looks for missing copies
SOCKET_TIMEOUT_SEC = 10
