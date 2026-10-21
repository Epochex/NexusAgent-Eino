"""Small real inference probe; never contacts a paid model service."""
import argparse
import json
import math
import pathlib
import urllib.request


def probe(base, token_file, model_dir, report):
    from transformers import AutoTokenizer
    token = pathlib.Path(token_file).read_text(encoding="utf-8").strip()
    tokenizer = AutoTokenizer.from_pretrained(model_dir, local_files_only=True, trust_remote_code=False)
    ids = tokenizer.encode(" sample" * 600, add_special_tokens=False)[:512]
    for count in range(512, 479, -1):
        stress = tokenizer.decode(ids[:count])
        stress_tokens = len(tokenizer.encode(stress))
        if stress_tokens == 512:
            break
    assert stress_tokens == 512, stress_tokens  # includes the tokenizer's framing

    def request(path, body=None):
        raw = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base + path, data=raw, headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=90) as response:
            return json.load(response)
    before = request("/metrics")
    measurements = []
    for texts in [["巴黎是法国的首都。"], [stress] * 4]:
        response = request("/v1/embeddings", {"model": "Qwen/Qwen3-Embedding-0.6B", "input": texts})
        assert len(response["data"]) == len(texts)
        norms = []
        for i, row in enumerate(response["data"]):
            assert row["index"] == i and len(row["embedding"]) == 1024
            assert all(math.isfinite(v) for v in row["embedding"])
            norm = math.sqrt(sum(v * v for v in row["embedding"]))
            assert abs(norm - 1) < .001
            norms.append(norm)
        measurements.append({"batch": len(texts), "input_tokens": response["usage"]["total_tokens"], "norms": norms})
    result = {"before": before, "after": request("/metrics"), "measurements": measurements, "stress_tokens_per_input": stress_tokens, "paid_api_calls": 0}
    pathlib.Path(report).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"measurements": measurements, "gpu": result["after"].get("gpu"), "peak_allocated_bytes": result["after"].get("peak_allocated_bytes"), "peak_reserved_bytes": result["after"].get("peak_reserved_bytes")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:18091")
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    probe(args.base, args.token_file, args.model_dir, args.report)
