import math
import re
from typing import Dict, List, Any

STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
    "they've", "this", "those", "through", "to", "too", "under", "until", "up",
    "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
    "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
    "yourself", "yourselves"
}

class SearchRanker:
    """
    Implements BM25 Relevance Ranking + PageRank Normalization
    Specification:
      k1 = 1.2, b = 0.75
      Final Score = 0.7 * (BM25 / max BM25) + 0.3 * (PageRank / max PageRank)
    """

    def __init__(self, k1: float = 1.2, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.inverted_index: Dict[str, Dict[str, int]] = {} # term -> {doc_id: tf}
        self.doc_catalog: Dict[str, Dict[str, Any]] = {}    # doc_id -> {title, url, doc_len, pagerank}
        self.total_docs: int = 0
        self.avgdl: float = 1.0
        self.max_pagerank: float = 1.0
        self.version: str = "none"

    def load_index(self, inverted_index, doc_catalog, total_docs, avgdl, max_pagerank, version="v0"):
        self.inverted_index = inverted_index
        self.doc_catalog = doc_catalog
        self.total_docs = total_docs
        self.avgdl = avgdl if avgdl > 0 else 1.0
        self.max_pagerank = max_pagerank if max_pagerank > 0 else 1.0
        self.version = version

    def tokenize(self, query: str) -> List[str]:
        raw_tokens = re.findall(r"[a-z0-9]+", query.lower())
        return [t for t in raw_tokens if len(t) >= 2 and t not in STOPWORDS]

    def compute_idf(self, df_t: int) -> float:
        # Standard Lucene/BM25 IDF: ln(1 + (N - df + 0.5) / (df + 0.5))
        numerator = self.total_docs - df_t + 0.5
        denominator = df_t + 0.5
        return math.log(1.0 + (numerator / max(1.0, denominator)))

    def search(self, query_str: str, top_k: int = 20) -> List[Dict[str, Any]]:
        tokens = self.tokenize(query_str)
        if not tokens or not self.inverted_index:
            return []

        # Find candidate documents containing at least one query term
        candidate_doc_ids = set()
        for t in tokens:
            if t in self.inverted_index:
                candidate_doc_ids.update(self.inverted_index[t].keys())

        if not candidate_doc_ids:
            return []

        # Calculate BM25 per candidate document
        bm25_scores: Dict[str, float] = {}
        for doc_id in candidate_doc_ids:
            doc_meta = self.doc_catalog.get(doc_id, {})
            doc_len = doc_meta.get("doc_len", self.avgdl)
            
            doc_score = 0.0
            for t in tokens:
                postings = self.inverted_index.get(t, {})
                if doc_id in postings:
                    tf = postings[doc_id]
                    df_t = len(postings)
                    idf = self.compute_idf(df_t)

                    # BM25 term score
                    norm_len = 1.0 - self.b + self.b * (doc_len / self.avgdl)
                    term_score = idf * (tf * (self.k1 + 1.0)) / (tf + self.k1 * norm_len)
                    doc_score += term_score

            bm25_scores[doc_id] = doc_score

        # Max values for normalization
        max_bm25 = max(bm25_scores.values()) if bm25_scores else 1.0
        if max_bm25 <= 0:
            max_bm25 = 1.0

        max_pr = self.max_pagerank if self.max_pagerank > 0 else 1.0

        # Combine: 0.7 * (BM25 / max BM25) + 0.3 * (PageRank / max PageRank)
        results = []
        for doc_id, bm25_val in bm25_scores.items():
            doc_meta = self.doc_catalog.get(doc_id, {})
            pr_val = doc_meta.get("pagerank", 0.15)

            norm_bm25 = bm25_val / max_bm25
            norm_pr = pr_val / max_pr
            final_score = (0.7 * norm_bm25) + (0.3 * norm_pr)

            results.append({
                "doc_id": doc_id,
                "title": doc_meta.get("title", f"Document {doc_id}"),
                "url": doc_meta.get("url", "#"),
                "bm25_score": round(bm25_val, 4),
                "norm_bm25": round(norm_bm25, 4),
                "pagerank": round(pr_val, 6),
                "norm_pagerank": round(norm_pr, 4),
                "final_score": round(final_score, 4)
            })

        # Rank descending by final_score
        results.sort(key=lambda x: x["final_score"], reverse=True)
        return results[:top_k]
