"""Unit tests for Unstructured Data & Hybrid Vector Ingestion Engine."""

import os
import sys
import math
import pytest

VECTOR_DIR = os.path.join(os.path.dirname(__file__), "..", "ai_vector_pipeline")
sys.path.insert(0, VECTOR_DIR)

from vector_ingestion_engine import DocumentChunker, EmbeddingGenerator, HybridVectorIndex


def test_document_chunker_basic():
    """Verify document chunker splits text and preserves metadata."""
    chunker = DocumentChunker(target_chunk_size=10, chunk_overlap=2)
    sample_text = (
        "This is paragraph one with several words for testing chunking behavior.\n\n"
        "This is paragraph two which should trigger chunk boundary segmentation."
    )
    metadata = {"source": "test_doc", "author": "architect"}
    chunks = chunker.chunk_text("doc_01", sample_text, metadata)

    assert len(chunks) >= 2
    for chunk in chunks:
        assert chunk["document_id"] == "doc_01"
        assert "chunk_id" in chunk
        assert "token_count" in chunk
        assert chunk["metadata"]["source"] == "test_doc"
        assert chunk["metadata"]["author"] == "architect"


def test_embedding_generator_dimension_and_norm():
    """Verify embedding generator produces normalized vectors of correct dimension."""
    dim = 64
    embedder = EmbeddingGenerator(dimension=dim)
    texts = ["Data lakehouse with Apache Iceberg and Snowflake", "Real-time streaming with Kafka and Spark"]
    embeddings = embedder.generate_batch(texts)

    assert len(embeddings) == 2
    for emb in embeddings:
        assert len(emb) == dim
        # Verify vector is unit normalized: sum(x^2) ≈ 1.0
        norm = math.sqrt(sum(x * x for x in emb))
        assert abs(norm - 1.0) < 1e-4


def test_hybrid_vector_index_search():
    """Verify dense, sparse BM25, and hybrid RRF search in vector index."""
    embedder = EmbeddingGenerator(dimension=64)
    index = HybridVectorIndex()

    docs = [
        {"chunk_id": "c1", "document_id": "d1", "text": "Apache Spark distributed batch data transformations", "metadata": {"tech": "spark"}},
        {"chunk_id": "c2", "document_id": "d2", "text": "Apache Kafka event streaming with consumer groups", "metadata": {"tech": "kafka"}},
        {"chunk_id": "c3", "document_id": "d3", "text": "Snowflake cloud data warehouse dimensional star schema", "metadata": {"tech": "snowflake"}},
    ]

    texts = [d["text"] for d in docs]
    embs = embedder.generate_batch(texts)
    index.insert_batch(docs, embs)

    assert len(index.chunks) == 3
    assert len(index.embeddings) == 3

    # Dense search
    q_emb = embedder.generate_batch(["Spark processing"])[0]
    dense_hits = index.dense_search(q_emb, top_k=2)
    assert len(dense_hits) <= 2

    # Sparse search
    sparse_hits = index.sparse_search("Kafka event streaming", top_k=2)
    assert len(sparse_hits) >= 1
    top_sparse_id, score = sparse_hits[0]
    assert top_sparse_id == "c2"

    # Hybrid RRF search
    hybrid_hits = index.hybrid_search("Kafka streaming", q_emb, top_n=2)
    assert len(hybrid_hits) >= 1
    assert "rrf_score" in hybrid_hits[0]
    assert "text" in hybrid_hits[0]

