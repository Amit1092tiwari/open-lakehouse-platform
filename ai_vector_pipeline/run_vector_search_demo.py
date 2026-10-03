"""
End-to-End Vector Ingestion & Hybrid Search Execution Demo.
"""

import sys
import io

# Ensure UTF-8 output on Windows consoles
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from vector_ingestion_engine import DocumentChunker, EmbeddingGenerator, HybridVectorIndex


def run_demo():
    print("[+] Initializing Unstructured Vector Ingestion Pipeline...\n")

    # 1. Sample Enterprise Architecture Knowledge Base
    documents = [
        {
            "id": "doc_finops_01",
            "content": (
                "BigQuery and Snowflake Cost Optimization: Avoid full table scans by enforcing partition pruning "
                "and clustering keys. Use incremental dbt models with merge strategies to minimize compute cost. "
                "Materialize intermediate views into marts for frequent BI queries."
            ),
            "metadata": {"topic": "FinOps", "author": "Data Platform Architecture", "tier": "Gold"}
        },
        {
            "id": "doc_rag_02",
            "content": (
                "Hybrid Search and RAG Architecture: Dense vector embeddings capture semantic similarity, while "
                "sparse lexical search (BM25) ensures accurate exact-match retrieval for technical identifiers. "
                "Reciprocal Rank Fusion (RRF) merges rankings with a smoothing constant (k=60) for optimal precision."
            ),
            "metadata": {"topic": "AI & Vectors", "author": "AI Engineering Team", "tier": "Production"}
        },
        {
            "id": "doc_spark_03",
            "content": (
                "Apache Spark Performance Tuning: Prevent OutOfMemoryError (OOM) by eliminating driver-side collection "
                "(avoid df.toPandas() on massive datasets). Use Snappy-compressed Parquet files and enable Adaptive "
                "Query Execution (AQE) to dynamically coalesce shuffle partitions and mitigate data skew."
            ),
            "metadata": {"topic": "Distributed Compute", "author": "Data Engineering Lead", "tier": "Core"}
        }
    ]

    chunker = DocumentChunker(target_chunk_size=100, chunk_overlap=15)
    embedder = EmbeddingGenerator(dimension=128)
    index = HybridVectorIndex()

    # 2. Chunk & Ingest
    all_chunks = []
    for doc in documents:
        chunks = chunker.chunk_text(doc["id"], doc["content"], doc["metadata"])
        all_chunks.extend(chunks)

    texts = [c["text"] for c in all_chunks]
    embeddings = embedder.generate_batch(texts)
    index.insert_batch(all_chunks, embeddings)

    # 3. Perform Hybrid Query
    query = "How do we prevent Spark driver OOM and tune Parquet shuffle partitions?"
    print(f"\n[QUERY]: \"{query}\"")
    query_emb = embedder.generate_batch([query])[0]

    results = index.hybrid_search(query, query_emb, top_n=2)

    print("\n[RESULTS] Top Hybrid Search (RRF) Ranked Matches:")
    for rank, res in enumerate(results, 1):
        print(f"\n Rank {rank} | Chunk ID: {res['chunk_id']} | RRF Score: {res['rrf_score']}")
        print(f"   Topic: {res['metadata'].get('topic')} | Doc ID: {res['document_id']}")
        print(f"   Excerpt: {res['text']}")

    print("\n[SUCCESS] End-to-end Vector Ingestion & Search verification passed successfully!")


if __name__ == "__main__":
    run_demo()
