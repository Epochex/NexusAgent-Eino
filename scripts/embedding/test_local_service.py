import threading
import tempfile
import hashlib
import pathlib
import unittest

from fastapi.testclient import TestClient
from local_service import create_app, verify_artifacts, MODEL, REVISION


class FakeEngine:
    model = "Qwen/Qwen3-Embedding-0.6B"

    def __init__(self):
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()
        self.block = False

    def embed(self, texts):
        if any(len(t.split()) > 512 for t in texts):
            raise ValueError("input exceeds 512 tokens; split before embedding")
        self.calls += 1
        if self.block:
            self.started.set()
            self.release.wait(3)
        return [[float(i), 1.0] for i in range(len(texts))], sum(len(t.split()) for t in texts)

    def metrics(self):
        return {"model": self.model, "device": "fixture", "calls": self.calls}


class LocalContractTests(unittest.TestCase):
    def test_artifact_identity_and_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            file = pathlib.Path(directory) / "config.json"
            file.write_bytes(b"fixed")
            lock = {"model": MODEL, "revision": REVISION, "files": {"config.json": {"sha256": hashlib.sha256(b"fixed").hexdigest()}}}
            verify_artifacts(directory, lock)
            file.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                verify_artifacts(directory, lock)

    def setUp(self):
        self.engine = FakeEngine()
        self.client = TestClient(create_app(self.engine, "private-test-token"))
        self.headers = {"Authorization": "Bearer private-test-token"}

    def post(self, texts, **extra):
        return self.client.post("/v1/embeddings", json={"model": self.engine.model, "input": texts, **extra}, headers=self.headers)

    def test_order_auth_and_bounds(self):
        self.assertEqual(self.client.post("/v1/embeddings", json={}).status_code, 401)
        for texts in [[], [""], ["a"] * 5, [1]]:
            self.assertEqual(self.post(texts).status_code, 400)
        self.assertEqual(self.post(["a"], model="wrong").status_code, 400)
        self.assertEqual(self.client.post("/v1/embeddings", content=b"x" * 65537, headers=self.headers).status_code, 413)
        self.assertEqual(self.engine.calls, 0)
        reply = self.post(["first text", "second"]).json()
        self.assertEqual([d["index"] for d in reply["data"]], [0, 1])
        self.assertEqual(reply["data"][1]["embedding"], [1.0, 1.0])
        self.assertEqual(reply["usage"]["total_tokens"], 3)

    def test_long_input_is_rejected_not_silently_truncated(self):
        self.assertEqual(self.post(["word " * 513]).status_code, 400)
        self.assertEqual(self.engine.calls, 0)

    def test_busy_request_has_bounded_rejection(self):
        self.engine.block = True
        result = []
        worker = threading.Thread(target=lambda: result.append(self.post(["one"]).status_code))
        worker.start()
        self.assertTrue(self.engine.started.wait(2))
        try:
            self.assertEqual(self.post(["two"]).status_code, 429)
        finally:
            self.engine.release.set()
            worker.join(4)
        self.assertEqual(result, [200])


if __name__ == "__main__":
    unittest.main()
