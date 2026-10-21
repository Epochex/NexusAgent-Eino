import unittest
from prepare_scifact import select


class DatasetTests(unittest.TestCase):
    def test_all_relevant_documents_retained_and_selection_reproducible(self):
        corpus = [{"_id": str(i), "title": str(i), "text": "content"} for i in range(8)]
        queries = [{"_id": "1", "text": "question"}]
        a = select(corpus, queries, [("1", "6", 1), ("1", "7", 1)], 4)
        b = select(list(reversed(corpus)), queries, [("1", "7", 1), ("1", "6", 1)], 4)
        self.assertEqual(a, b)
        self.assertTrue({"6", "7"}.issubset({d["id"] for d in a[0]}))
        self.assertEqual(a[1][0]["relevant"], ["6", "7"])

    def test_inconsistent_labels_fail_closed(self):
        with self.assertRaises(ValueError):
            select([], [{"_id": "1", "text": "q"}], [("1", "missing", 1)], 4)


if __name__ == "__main__":
    unittest.main()
