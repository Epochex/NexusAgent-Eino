"""Resumable verified public artifact download for slow large-file proxies."""
import concurrent.futures
import hashlib
import pathlib
import re
import time
import urllib.request


def download(url, target, expected_sha256, workers=8):
    target = pathlib.Path(target)
    if target.exists():
        with target.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() == expected_sha256:
                return
    with urllib.request.urlopen(urllib.request.Request(url, headers={"Range": "bytes=0-0"}), timeout=60) as response:
        content_range = response.headers.get("Content-Range", "")
        match = re.fullmatch(r"bytes 0-0/(\d+)", content_range)
        if response.status != 206 or not match:
            raise ValueError("server did not honor the exact probe range")
        size = int(match.group(1))
    chunks = target.with_name(target.name + ".ranges")
    chunks.mkdir(parents=True, exist_ok=True)
    step = 8 * 1024 * 1024
    ranges = [(start, min(start + step, size) - 1) for start in range(0, size, step)]

    def fetch(bounds):
        start, end = bounds
        part = chunks / str(start)
        if part.exists() and part.stat().st_size == end - start + 1:
            return part
        for attempt in range(4):
            try:
                request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
                with urllib.request.urlopen(request, timeout=90) as response:
                    if response.status != 206 or response.headers.get("Content-Range") != f"bytes {start}-{end}/{size}":
                        raise ValueError("server returned the wrong artifact range")
                    data = response.read(end - start + 2)
                    if len(data) != end - start + 1:
                        raise ValueError("short artifact range")
                part.write_bytes(data)
                return part
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(attempt + 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for index, _ in enumerate(pool.map(fetch, ranges), 1):
            if index % 16 == 0 or index == len(ranges):
                print(f"{target.name}: {index}/{len(ranges)} verified-length ranges", flush=True)
    assembled = target.with_name(target.name + ".assembled")
    digest = hashlib.sha256()
    with assembled.open("wb") as out:
        for start, end in ranges:
            with (chunks / str(start)).open("rb") as part:
                while data := part.read(1024 * 1024):
                    out.write(data)
                    digest.update(data)
    if digest.hexdigest() != expected_sha256:
        raise ValueError("complete artifact SHA256 mismatch")
    assembled.replace(target)
    print("Publisher SHA256 verified:", target.name, flush=True)
