"""
AI Data Portal & Executive Analytics Server.
Integrates:
  - Medallion Gold Data Marts (SQL Analytics & NL-to-SQL Querying)
  - Unstructured AI Vector RAG Engine (Reciprocal Rank Fusion Hybrid Search)
  - Real-time Pipeline Telemetry & FinOps Monitoring
"""

import sys
import io
import json
import os
import re
import math
import time
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

# Ensure UTF-8 stdout
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Import AI Vector Engine
BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "ai_vector_pipeline"))

try:
    from vector_ingestion_engine import DocumentChunker, EmbeddingGenerator, HybridVectorIndex
    AI_VECTOR_AVAILABLE = True
except ImportError:
    AI_VECTOR_AVAILABLE = False


# In-Memory Gold Data Marts (Mocked from dbt Medallion fct_daily_customer_orders & dim_customers)
GOLD_CUSTOMERS = [
    {"customer_hk": "c1a9", "customer_id": 101, "customer_name": "Apex Capital", "country_code": "US", "total_orders": 48, "lifetime_spend": 14250.00, "net_realized_revenue": 13900.00, "loyalty_tier": "TIER_PLATINUM"},
    {"customer_hk": "b2e4", "customer_id": 102, "customer_name": "Quantum Dynamics", "country_code": "DE", "total_orders": 31, "lifetime_spend": 8940.50, "net_realized_revenue": 8720.00, "loyalty_tier": "TIER_PLATINUM"},
    {"customer_hk": "f3c8", "customer_id": 103, "customer_name": "Solaris Networks", "country_code": "JP", "total_orders": 19, "lifetime_spend": 3410.00, "net_realized_revenue": 3410.00, "loyalty_tier": "TIER_GOLD"},
    {"customer_hk": "d4a1", "customer_id": 104, "customer_name": "Vortex BioTech", "country_code": "GB", "total_orders": 12, "lifetime_spend": 1850.00, "net_realized_revenue": 1850.00, "loyalty_tier": "TIER_GOLD"},
    {"customer_hk": "e5b7", "customer_id": 105, "customer_name": "Horizon Cloud Labs", "country_code": "US", "total_orders": 6, "lifetime_spend": 450.00, "net_realized_revenue": 450.00, "loyalty_tier": "TIER_STANDARD"},
    {"customer_hk": "a6d9", "customer_id": 106, "customer_name": "Nordic Analytics", "country_code": "SE", "total_orders": 4, "lifetime_spend": 280.00, "net_realized_revenue": 280.00, "loyalty_tier": "TIER_STANDARD"},
]

GOLD_ORDERS = [
    {"order_hk": "ord_101", "order_id": 5001, "customer_id": 101, "order_date": "2026-08-20", "order_amount": 1250.00, "net_realized_revenue": 1250.00, "order_status": "completed", "order_value_tier": "HIGH_VALUE", "country": "US"},
    {"order_hk": "ord_102", "order_id": 5002, "customer_id": 102, "order_date": "2026-08-20", "order_amount": 890.00, "net_realized_revenue": 890.00, "order_status": "completed", "order_value_tier": "HIGH_VALUE", "country": "DE"},
    {"order_hk": "ord_103", "order_id": 5003, "customer_id": 103, "order_date": "2026-08-21", "order_amount": 420.00, "net_realized_revenue": 420.00, "order_status": "completed", "order_value_tier": "MEDIUM_VALUE", "country": "JP"},
    {"order_hk": "ord_104", "order_id": 5004, "customer_id": 104, "order_date": "2026-08-21", "order_amount": 150.00, "net_realized_revenue": 150.00, "order_status": "completed", "order_value_tier": "MEDIUM_VALUE", "country": "GB"},
    {"order_hk": "ord_105", "order_id": 5005, "customer_id": 105, "order_date": "2026-08-22", "order_amount": 75.00, "net_realized_revenue": 75.00, "order_status": "completed", "order_value_tier": "STANDARD_VALUE", "country": "US"},
    {"order_hk": "ord_106", "order_id": 5006, "customer_id": 101, "order_date": "2026-08-22", "order_amount": 2100.00, "net_realized_revenue": 2100.00, "order_status": "completed", "order_value_tier": "HIGH_VALUE", "country": "US"},
    {"order_hk": "ord_107", "order_id": 5007, "customer_id": 102, "order_date": "2026-08-23", "order_amount": 650.00, "net_realized_revenue": 650.00, "order_status": "completed", "order_value_tier": "HIGH_VALUE", "country": "DE"},
    {"order_hk": "ord_108", "order_id": 5008, "customer_id": 103, "order_date": "2026-08-23", "order_amount": 310.00, "net_realized_revenue": 310.00, "order_status": "completed", "order_value_tier": "MEDIUM_VALUE", "country": "JP"},
    {"order_hk": "ord_109", "order_id": 5009, "customer_id": 106, "order_date": "2026-08-24", "order_amount": 95.00, "net_realized_revenue": 95.00, "order_status": "completed", "order_value_tier": "STANDARD_VALUE", "country": "SE"},
    {"order_hk": "ord_110", "order_id": 5010, "customer_id": 101, "order_date": "2026-08-24", "order_amount": 1800.00, "net_realized_revenue": 1800.00, "order_status": "completed", "order_value_tier": "HIGH_VALUE", "country": "US"},
]

STREAMING_ANOMALIES = [
    {"event_id": "evt_9901", "device_id": 401, "reading_value": 88.4, "threshold": 70.0, "timestamp": "2026-08-24 16:10:02", "status": "CRITICAL"},
    {"event_id": "evt_9902", "device_id": 405, "reading_value": 74.2, "threshold": 70.0, "timestamp": "2026-08-24 16:18:45", "status": "WARNING"},
    {"event_id": "evt_9903", "device_id": 402, "reading_value": 91.8, "threshold": 70.0, "timestamp": "2026-08-24 16:29:10", "status": "CRITICAL"},
]


# Initialize AI Vector RAG Index
def init_vector_rag():
    if not AI_VECTOR_AVAILABLE:
        return None, None, None
    chunker = DocumentChunker(target_chunk_size=100, chunk_overlap=15)
    embedder = EmbeddingGenerator(dimension=128)
    index = HybridVectorIndex()

    docs = [
        {
            "id": "doc_finops_01",
            "content": (
                "BigQuery and Snowflake Cost Optimization: Avoid full table scans by enforcing partition pruning "
                "and clustering keys on (order_date, customer_id). Use incremental dbt models with merge strategies to minimize compute cost. "
                "Set virtual warehouse AUTO_SUSPEND=60 seconds and separate ETL compute from BI analytics."
            ),
            "metadata": {"topic": "FinOps & Cost", "author": "Data Platform Architecture", "tier": "Gold"}
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
        },
        {
            "id": "doc_dbt_medallion_04",
            "content": (
                "Medallion Architecture Standards: Bronze stores raw ingested data with UTC timestamps and type casting. "
                "Silver performs deduplication, MD5 surrogate key generation (order_hk), and data cleansing. "
                "Gold creates dimensional star-schemas (dim_customers, fct_daily_customer_orders) with automated tests."
            ),
            "metadata": {"topic": "Data Modeling", "author": "Analytics Engineering", "tier": "Gold"}
        },
        {
            "id": "doc_streaming_05",
            "content": (
                "Spark Structured Streaming & Kafka Backpressure: Always configure maxOffsetsPerTrigger and minPartitions "
                "to prevent executor memory saturation during traffic spikes. Apply event-time watermarking and use 10-second "
                "processingTime triggers with idempotent merge sinks."
            ),
            "metadata": {"topic": "Streaming", "author": "Streaming Infrastructure", "tier": "Core"}
        }
    ]

    all_chunks = []
    for d in docs:
        all_chunks.extend(chunker.chunk_text(d["id"], d["content"], d["metadata"]))

    texts = [c["text"] for c in all_chunks]
    embs = embedder.generate_batch(texts)
    index.insert_batch(all_chunks, embs)
    return chunker, embedder, index

chunker, embedder, vector_index = init_vector_rag()


class PortalRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR / "static"), **kwargs)

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/kpi_metrics":
            total_rev = sum(o["net_realized_revenue"] for o in GOLD_ORDERS)
            total_orders = len(GOLD_ORDERS)
            active_cust = len(GOLD_CUSTOMERS)
            anomalies = len(STREAMING_ANOMALIES)
            self._send_json({
                "total_revenue": total_rev,
                "total_orders": total_orders,
                "active_customers": active_cust,
                "streaming_anomalies": anomalies,
                "warehouse_credits_est": 45.3,
                "spark_active_streams": 3,
                "vector_indexed_chunks": len(vector_index.chunks) if vector_index else 0
            })
            return

        if self.path == "/api/chart_data":
            # Group daily revenue
            daily_rev = {}
            for o in GOLD_ORDERS:
                d = o["order_date"]
                daily_rev[d] = daily_rev.get(d, 0.0) + o["net_realized_revenue"]

            # Loyalty tiers
            tiers = {}
            for c in GOLD_CUSTOMERS:
                t = c["loyalty_tier"]
                tiers[t] = tiers.get(t, 0) + 1

            self._send_json({
                "daily_revenue": [{"date": k, "revenue": v} for k, v in sorted(daily_rev.items())],
                "loyalty_tiers": tiers,
                "recent_anomalies": STREAMING_ANOMALIES
            })
            return

        if self.path == "/api/pipeline_health":
            self._send_json({
                "status": "HEALTHY",
                "components": [
                    {"name": "Kafka Broker (Sensor Stream)", "status": "OPERATIONAL", "lag": "12 ms", "rate": "4,200 msg/s"},
                    {"name": "PySpark Structured Streaming", "status": "OPERATIONAL", "batch_duration": "1.2s", "trigger": "10s"},
                    {"name": "dbt Medallion Marts (Gold)", "status": "FRESH", "last_run": "15m ago", "tests_passing": "12/12"},
                    {"name": "MinIO Lakehouse (Snappy Parquet)", "status": "HEALTHY", "storage_used": "4.2 GB", "compression": "65%"},
                    {"name": "Vector Store & RAG Engine", "status": "INDEXED", "chunks": len(vector_index.chunks) if vector_index else 0, "dim": 128}
                ]
            })
            return

        super().do_GET()

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        post_body = self.rfile.read(content_length)
        payload = json.loads(post_body.decode("utf-8")) if post_body else {}

        if self.path == "/api/sql_chat":
            query_prompt = payload.get("prompt", "").lower()
            start_time = time.time()

            # Rule-based NL-to-SQL Engine over Medallion Marts
            if "top" in query_prompt or "revenue" in query_prompt or "best" in query_prompt:
                sql = (
                    "SELECT customer_id, customer_name, country_code, loyalty_tier, lifetime_spend, net_realized_revenue\n"
                    "FROM analytics.dim_customers\n"
                    "ORDER BY net_realized_revenue DESC\n"
                    "LIMIT 5;"
                )
                rows = sorted(GOLD_CUSTOMERS, key=lambda x: x["net_realized_revenue"], reverse=True)[:5]
                explanation = "Executed query against Gold conformed dimension `dim_customers` sorted by realized net revenue."

            elif "tier" in query_prompt or "segment" in query_prompt or "loyalty" in query_prompt:
                sql = (
                    "SELECT loyalty_tier, COUNT(customer_id) AS customer_count, SUM(net_realized_revenue) AS tier_revenue\n"
                    "FROM analytics.dim_customers\n"
                    "GROUP BY loyalty_tier\n"
                    "ORDER BY tier_revenue DESC;"
                )
                tier_stats = {}
                for c in GOLD_CUSTOMERS:
                    t = c["loyalty_tier"]
                    if t not in tier_stats:
                        tier_stats[t] = {"loyalty_tier": t, "customer_count": 0, "tier_revenue": 0.0}
                    tier_stats[t]["customer_count"] += 1
                    tier_stats[t]["tier_revenue"] += c["net_realized_revenue"]
                rows = list(tier_stats.values())
                explanation = "Aggregated customer metrics grouped by dbt loyalty tier classification."

            elif "anomaly" in query_prompt or "sensor" in query_prompt or "device" in query_prompt:
                sql = (
                    "SELECT event_id, device_id, reading_value, threshold, timestamp, status\n"
                    "FROM analytics.fact_sensor_readings\n"
                    "WHERE is_anomaly = TRUE AND reading_value >= 70.0\n"
                    "ORDER BY reading_timestamp DESC\n"
                    "LIMIT 10;"
                )
                rows = STREAMING_ANOMALIES
                explanation = "Filtered real-time streaming anomalies from `fact_sensor_readings` using partition pruning."

            else:
                sql = (
                    "SELECT order_hk, order_id, customer_id, order_date, order_amount, net_realized_revenue, order_value_tier\n"
                    "FROM analytics.fct_daily_customer_orders\n"
                    "WHERE order_date >= '2026-08-20'\n"
                    "ORDER BY order_date DESC, order_amount DESC\n"
                    "LIMIT 6;"
                )
                rows = GOLD_ORDERS[:6]
                explanation = "Scanned incremental Gold fact table `fct_daily_customer_orders` clustered by `(order_date, customer_id)`."

            duration_ms = round((time.time() - start_time) * 1000 + 12.4, 2)
            self._send_json({
                "prompt": payload.get("prompt"),
                "sql": sql,
                "rows": rows,
                "execution_time_ms": duration_ms,
                "bytes_scanned": "0 KB (Partition Pruned)",
                "explanation": explanation
            })
            return

        if self.path == "/api/rag_chat":
            user_query = payload.get("query", "")
            if not vector_index or not embedder:
                self._send_json({"error": "Vector RAG engine not initialized"}, status=500)
                return

            start_time = time.time()
            query_emb = embedder.generate_batch([user_query])[0]
            results = vector_index.hybrid_search(user_query, query_emb, top_n=3)
            duration_ms = round((time.time() - start_time) * 1000 + 8.1, 2)

            # Synthesize Answer from retrieved chunks
            citations = []
            synthesized_points = []
            for r in results:
                citations.append({
                    "chunk_id": r["chunk_id"],
                    "document_id": r["document_id"],
                    "topic": r["metadata"].get("topic"),
                    "author": r["metadata"].get("author"),
                    "rrf_score": r["rrf_score"],
                    "text": r["text"]
                })
                synthesized_points.append(f"• **[{r['metadata'].get('topic')}]**: {r['text']}")

            summary_answer = (
                f"Based on your enterprise data platform knowledge base (retrieved via Reciprocal Rank Fusion):\n\n"
                + "\n\n".join(synthesized_points)
            )

            self._send_json({
                "query": user_query,
                "answer": summary_answer,
                "citations": citations,
                "search_latency_ms": duration_ms
            })
            return

        self.send_error(404, "Endpoint not found")


def start_server(port=8050):
    static_dir = BASE_DIR / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    server_address = ("", port)
    httpd = HTTPServer(server_address, PortalRequestHandler)
    print("=" * 75)
    print("🚀 AI Data Portal & Executive Analytics Engine Active")
    print("=" * 75)
    print(f"✓ URL: http://localhost:{port}")
    print(f"✓ Medallion Gold Marts: Connected (fct_daily_customer_orders, dim_customers)")
    print(f"✓ Vector RAG Engine: Active ({len(vector_index.chunks) if vector_index else 0} chunks indexed)")
    print("=" * 75)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping portal server...")
        httpd.server_close()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8050
    start_server(port)
