"""Deterministic BEIR SciFact subset; downloaded text remains outside Git."""
import argparse
import csv
import hashlib
import io
import json
import pathlib
import zipfile

SOURCE = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip"
ARCHIVE_SHA256 = "536e14446a0ba56ed1398ab1055f39fe852686ecad24a6306c80c490fa8e0165"


def select(corpus, queries, labels, count=500):
    by_doc = {str(d["_id"]): d for d in corpus}
    by_query = {str(q["_id"]): q for q in queries}
    gold = {}
    for qid, did, score in labels:
        if score <= 0:
            continue
        if qid not in by_query or did not in by_doc:
            raise ValueError("qrels reference missing query/document")
        gold.setdefault(qid, set()).add(did)
    selected = set().union(*gold.values()) if gold else set()
    if not selected or len(selected) > count:
        raise ValueError("subset cannot retain all positive documents")
    # Selection is deterministic and explicitly qrel-aware, not a full-corpus
    # leaderboard result. All annotated positives are retained; hash-order fills
    # the remaining candidate documents without looking at retrieval scores.
    for did in sorted(by_doc, key=lambda d: hashlib.sha256(d.encode()).hexdigest()):
        if len(selected) >= count:
            break
        selected.add(did)
    documents = [{"id": d, "content": (by_doc[d]["title"] + "\n" + by_doc[d]["text"]).strip()} for d in sorted(selected)]
    cases = [{"id": q, "question": by_query[q]["text"], "kb": "primary", "relevant": sorted(gold[q]), "category": "answerable"} for q in sorted(gold, key=int)]
    return documents, cases


def prepare(archive, output):
    digest = hashlib.file_digest(archive.open("rb"), "sha256").hexdigest()
    if digest != ARCHIVE_SHA256:
        raise ValueError("SciFact archive SHA256 differs; inspect before changing the pin")
    with zipfile.ZipFile(archive) as z:
        corpus = [json.loads(l) for l in z.read("scifact/corpus.jsonl").decode().splitlines()]
        queries = [json.loads(l) for l in z.read("scifact/queries.jsonl").decode().splitlines()]
        rows = csv.DictReader(io.StringIO(z.read("scifact/qrels/test.tsv").decode()), delimiter="\t")
        labels = [(r["query-id"], r["corpus-id"], int(r["score"])) for r in rows]
    documents, cases = select(corpus, queries, labels)
    output.mkdir(parents=True, exist_ok=True)
    (output / "corpus.json").write_text(json.dumps(documents, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "queries.jsonl").write_text("\n".join(json.dumps(c, ensure_ascii=False) for c in cases) + "\n", encoding="utf-8")
    manifest = {"dataset": "BEIR SciFact test, qrel-aware 500-document subset", "source": SOURCE, "source_sha256": digest, "dataset_card": "https://huggingface.co/datasets/BeIR/scifact", "dataset_license_as_listed": "CC-BY-SA-4.0", "full_corpus_documents": len(corpus), "selected_documents": len(documents), "queries": len(cases), "qrel_policy": "all test positive documents retained; deterministic SHA256-order distractors", "leaderboard_comparable": False}
    for name in ["corpus.json", "queries.jsonl"]:
        manifest[name + "_sha256"] = hashlib.sha256((output / name).read_bytes()).hexdigest()
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    prepare(args.archive, args.output)
