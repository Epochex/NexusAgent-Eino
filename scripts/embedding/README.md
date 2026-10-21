# Local embedding acceptance fixture

This serves a pinned Qwen3-Embedding-0.6B on loopback for this project's real
retrieval/RAG verification. It is not an internet-facing inference platform.
The Go production embedding interface is unchanged. The supported request
contract is a model ID plus an array of 1–4 nonempty strings; each input must fit
512 actual tokenizer tokens. Longer text fails explicitly, without truncation.

## Environment and model

Use a separate Python 3.11 environment. On the verified Windows RTX5060 setup:

```powershell
uv venv output/local-embedding/venv --python 3.11
uv pip install --python output/local-embedding/venv/Scripts/python.exe torch==2.8.0 --index-url https://download.pytorch.org/whl/cu128
uv pip install --python output/local-embedding/venv/Scripts/python.exe -r scripts/embedding/requirements.txt
python scripts/embedding/download_model.py output/local-embedding/model
```

Large weights use bounded resumable ranges with exact Content-Range and whole
publisher SHA256 verification. Model/tokenizer files are pinned in
`artifact-lock.json` and verified again before loading. No remote model code is
enabled. The CUDA wheel, environment, model files and download ranges belong in
the ignored output directory, not Git.

Create a private bearer-token file under an access-restricted local directory.
Do not paste its contents into commands or Git. Start from the repo root:

```powershell
$env:HF_HUB_OFFLINE = '1'
output/local-embedding/venv/Scripts/python.exe scripts/embedding/local_service.py `
  --model-dir output/local-embedding/model --token-file <private-token-file>
```

The listener is always `127.0.0.1:18091` (port configurable). `/healthz` reports
ready state; `/v1/embeddings` and `/metrics` require the private bearer. Only one
GPU batch is allowed in flight; a concurrent inference request receives429.
For the application, configure model `Qwen/Qwen3-Embedding-0.6B`, dimension1024,
provider URL `http://127.0.0.1:18091/v1`, RAG embedding batch at most4 and a
compatible chunk size. CPU mode must be explicitly requested; no automatic
fallback hides a failed CUDA installation.

Authorize only this exact origin through the application's existing outbound
policy when configuring a private endpoint. The service is deliberately sized
for local verification; don't point an unrestricted fleet of workers at one GPU.

The model uses left-padding, last-token pooling, FP16 CUDA inference, no KV
cache and normalized FP32 output. This baseline applies no query instruction:
the existing production embedding contract carries the same model for document
and query inputs. It does not measure Qwen's instruction-aware retrieval gain.

## Verification

Zero-model/zero-credit contract and dataset tests:

```powershell
python -m unittest discover -s scripts/embedding -p 'test_*.py'
```

Actual inference and peak-memory probe:

```powershell
output/local-embedding/venv/Scripts/python.exe scripts/embedding/probe.py `
  --model-dir output/local-embedding/model --token-file <private-token-file> `
  --report output/local-embedding/probe.json
```

The public-model evaluation uses the BEIR SciFact test qrels and an explicitly
qrel-aware500-document subset of its5183-document corpus. Download the archive
from the pinned URL in `prepare_scifact.py`, then:

```powershell
python scripts/embedding/prepare_scifact.py output/local-embedding/scifact.zip output/local-embedding/scifact500
```

[BEIR SciFact dataset card](https://huggingface.co/datasets/BeIR/scifact) lists
CC-BY-SA-4.0; see also [BEIR](https://github.com/beir-cellar/beir) and the
[original SciFact project](https://github.com/allenai/scifact). Retain their
attribution and license when redistributing data. This repo publishes preparation
code, hashes, query/document IDs and scores; it does not bundle the corpus text.
The Qwen model is Apache-2.0, separately from this repo's upstream-code licensing.

For Go retrieval tests set `NX_LOCAL_EMBEDDING_URL`,
`NX_LOCAL_EMBEDDING_TOKEN_FILE`, `NX_LOCAL_EMBEDDING_REPORT_DIR`,
`NX_SCIFACT_SUBSET_DIR`, `NX_TEST_DATABASE_DSN` and `NX_TEST_MILVUS_ADDR`.
Use an explicit disposable database ending `_test` and the acceptance
infrastructure. From backend:

```powershell
go test -race -tags 'integration infrastructure localembedding' ./internal/service -run '^TestLocalEmbeddingSemanticRetrieval$' -count=1 -timeout=45m -v
```

This creates isolated publication jobs and real split/model/Milvus vectors;
it does not pretend its fixture indexing is authenticated upload/async-worker
coverage. That path is covered by the separate opt-in RAG acceptance. Any DS
generation in that acceptance uses an explicitly supplied existing spend ledger
and never resets it. Ordinary CI loads neither the model nor the credentials.
