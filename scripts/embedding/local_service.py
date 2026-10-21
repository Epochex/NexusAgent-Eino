"""Bounded loopback-only model fixture, not an internet-facing serving stack."""
import argparse
import hmac
import hashlib
import json
import logging
import pathlib
import os
import threading
import time

from fastapi import FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool

MODEL = "Qwen/Qwen3-Embedding-0.6B"
REVISION = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"


def verify_artifacts(model_dir, lock=None):
    lock = lock or json.loads(pathlib.Path(__file__).with_name("artifact-lock.json").read_text(encoding="utf-8"))
    if lock.get("model") != MODEL or lock.get("revision") != REVISION:
        raise ValueError("model artifact identity mismatch")
    for name, info in lock["files"].items():
        path = pathlib.Path(model_dir) / name
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if digest != info["sha256"]:
            raise ValueError("model artifact checksum mismatch: " + name)


def create_app(engine, token):
    if not token:
        raise ValueError("private bearer token required")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    slot = threading.Lock()

    def authorize(request):
        if not hmac.compare_digest(request.headers.get("authorization", ""), "Bearer " + token):
            raise HTTPException(401, "unauthorized")

    @app.get("/healthz")
    async def health():
        return {"ready": True, "model": engine.model, "revision": REVISION}

    @app.get("/metrics")
    async def metrics(request: Request):
        authorize(request)
        return engine.metrics()

    @app.post("/v1/embeddings")
    async def embeddings(request: Request):
        authorize(request)
        raw = bytearray()
        async for part in request.stream():
            raw.extend(part)
            if len(raw) > 65536:
                raise HTTPException(413, "input body exceeds 64 KiB")
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeError):
            raise HTTPException(400, "invalid JSON")
        if not isinstance(body, dict) or body.get("model") != engine.model:
            raise HTTPException(400, "model mismatch")
        texts = body.get("input")
        if not isinstance(texts, list) or not 1 <= len(texts) <= 4 or any(not isinstance(t, str) or not t.strip() for t in texts):
            raise HTTPException(400, "input must be 1..4 nonempty strings")
        if not slot.acquire(blocking=False):
            raise HTTPException(429, "one GPU batch already in flight")
        try:
            vectors, tokens = await run_in_threadpool(engine.embed, texts)
        except ValueError as error:
            raise HTTPException(400, str(error))
        except Exception:
            logging.exception("local embedding inference failed")
            raise HTTPException(503, "local inference failed; check private service log")
        finally:
            slot.release()
        return {"object": "list", "model": engine.model, "data": [{"object": "embedding", "index": i, "embedding": v} for i, v in enumerate(vectors)], "usage": {"prompt_tokens": tokens, "total_tokens": tokens}}

    return app


class Encoder:
    model = MODEL

    def __init__(self, model_dir, device):
        verify_artifacts(model_dir)
        import torch
        import transformers
        from transformers import AutoModel, AutoTokenizer
        self.torch = torch
        self.device = device
        if device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA required; CPU fallback must be explicitly selected")
        torch.set_num_threads(2)
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, padding_side="left", local_files_only=True, trust_remote_code=False)
        self.network = AutoModel.from_pretrained(model_dir, local_files_only=True, trust_remote_code=False, torch_dtype=torch.float16 if device == "cuda" else torch.float32, attn_implementation="sdpa").to(device).eval()
        if device == "cuda":
            torch.cuda.synchronize()
            torch.cuda.reset_peak_memory_stats()
        self.calls = self.tokens = self.items = self.failures = 0
        self.latencies = []
        self.versions = {"torch": torch.__version__, "cuda": torch.version.cuda, "transformers": transformers.__version__}

    def embed(self, texts):
        torch = self.torch
        inputs = self.tokenizer(texts, padding=True, truncation=False, return_tensors="pt")
        lengths = inputs["attention_mask"].sum(dim=1)
        if int(lengths.max()) > 512:
            raise ValueError("input exceeds 512 tokens; split before embedding (no silent truncation)")
        tokens = int(lengths.sum())
        started = time.perf_counter()
        try:
            with torch.inference_mode():
                result = self.network(**{k: v.to(self.device) for k, v in inputs.items()}, use_cache=False).last_hidden_state[:, -1]
                result = torch.nn.functional.normalize(result.float(), p=2, dim=1)
                if not bool(torch.isfinite(result).all()):
                    raise RuntimeError("non-finite model output")
                vectors = result.cpu().tolist()
        except Exception:
            self.failures += 1
            if self.device == "cuda":
                torch.cuda.empty_cache()
            raise
        elapsed = (time.perf_counter() - started) * 1000
        self.calls += 1
        self.items += len(texts)
        self.tokens += tokens
        if len(self.latencies) < 20000:
            self.latencies.append(elapsed)
        return vectors, tokens

    def metrics(self):
        torch = self.torch
        result = {"model": self.model, "revision": REVISION, "device": self.device, "dtype": str(next(self.network.parameters()).dtype), "versions": self.versions, "calls": self.calls, "items": self.items, "input_tokens": self.tokens, "inference_failures": self.failures, "dimension": self.network.config.hidden_size, "max_input_tokens": 512, "max_batch": 4, "query_instruction": "none; identical production embedding contract for documents and queries", "inference_ms": self.latencies, "cpu_threads": torch.get_num_threads(), "cpu_user_seconds": os.times().user, "cpu_system_seconds": os.times().system}
        if self.device == "cuda":
            result.update(gpu=torch.cuda.get_device_name(), allocated_bytes=torch.cuda.memory_allocated(), reserved_bytes=torch.cuda.memory_reserved(), peak_allocated_bytes=torch.cuda.max_memory_allocated(), peak_reserved_bytes=torch.cuda.max_memory_reserved())
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--port", type=int, default=18091)
    parser.add_argument("--device", choices=["cuda", "cpu"], default="cuda")
    args = parser.parse_args()
    engine = Encoder(args.model_dir, args.device)
    token = pathlib.Path(args.token_file).read_text(encoding="utf-8").strip()
    import uvicorn
    uvicorn.run(create_app(engine, token), host="127.0.0.1", port=args.port, workers=1, limit_concurrency=8, timeout_keep_alive=5, access_log=False)


if __name__ == "__main__":
    main()
