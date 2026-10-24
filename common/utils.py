import sys
import time


def log(component, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{component}] {msg}", file=sys.stdout, flush=True)


def human_bytes(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
