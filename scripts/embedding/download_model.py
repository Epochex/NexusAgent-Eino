"""Download fixed public safetensors assets; verify the publisher's weight hash."""
import argparse
import hashlib
import json
import pathlib
import urllib.request

MODEL = "Qwen/Qwen3-Embedding-0.6B"
REVISION = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
FILES = ["config.json", "tokenizer.json", "tokenizer_config.json", "merges.txt", "vocab.json", "model.safetensors"]


def download(destination):
    destination.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"https://huggingface.co/api/models/{MODEL}/revision/{REVISION}?blobs=true", timeout=60) as response:
        metadata = json.load(response)
    expected = {f["rfilename"]: f for f in metadata["siblings"]}
    manifest = {"model": MODEL, "revision": REVISION, "license": metadata.get("cardData", {}).get("license"), "files": {}}
    for name in FILES:
        path = destination / name
        publisher_hash = expected[name].get("lfs", {}).get("sha256")
        if publisher_hash:
            from ranged_download import download as ranged_download
            ranged_download(f"https://huggingface.co/{MODEL}/resolve/{REVISION}/{name}?download=true",path,publisher_hash)
        if not path.exists():
            part = path.with_suffix(path.suffix + ".partial")
            print("Downloading", name, flush=True)
            with urllib.request.urlopen(f"https://huggingface.co/{MODEL}/resolve/{REVISION}/{name}?download=true", timeout=120) as response, part.open("wb") as out:
                while chunk := response.read(1024 * 1024):
                    out.write(chunk)
            part.replace(path)
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if publisher_hash and digest != publisher_hash:
            raise RuntimeError(f"publisher SHA256 mismatch: {name}")
        manifest["files"][name] = {"sha256": digest, "bytes": path.stat().st_size, "publisher_sha256": publisher_hash}
        print("Verified", name, path.stat().st_size, flush=True)
    (destination / "artifact-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=pathlib.Path)
    download(parser.parse_args().destination)
