"""Basic pipeline configuration and structure tests."""

import os

ROOT = os.path.join(os.path.dirname(__file__), "..")


def test_env_example_exists():
    """Verify .env.example file exists."""
    assert os.path.exists(os.path.join(ROOT, ".env.example"))


def test_docker_compose_exists():
    """Verify docker-compose.yaml exists."""
    assert os.path.exists(os.path.join(ROOT, "docker-compose.yaml"))


def test_dockerignore_exists():
    """Verify .dockerignore exists."""
    assert os.path.exists(os.path.join(ROOT, ".dockerignore"))


def test_makefile_exists():
    """Verify Makefile exists."""
    assert os.path.exists(os.path.join(ROOT, "Makefile"))


def test_airflow_dags_exist():
    """Verify Airflow DAG files exist."""
    dag_dir = os.path.join(ROOT, "airflow", "dags")
    assert os.path.exists(os.path.join(dag_dir, "batch_ingestion_dag.py"))
    assert os.path.exists(os.path.join(dag_dir, "streaming_monitoring_dag.py"))
    assert os.path.exists(os.path.join(dag_dir, "warehouse_transform_dag.py"))


def test_spark_jobs_exist():
    """Verify Spark job files exist."""
    spark_dir = os.path.join(ROOT, "spark")
    assert os.path.exists(os.path.join(spark_dir, "spark_batch_job.py"))
    assert os.path.exists(os.path.join(spark_dir, "spark_streaming_job.py"))


def test_kafka_producer_exists():
    """Verify Kafka producer file exists."""
    assert os.path.exists(os.path.join(ROOT, "kafka", "producer.py"))


def test_init_sql_exists():
    """Verify database initialization script exists."""
    assert os.path.exists(os.path.join(ROOT, "scripts", "init_db.sql"))


def test_dockerfiles_exist():
    """Verify all Dockerfiles exist."""
    assert os.path.exists(os.path.join(ROOT, "airflow", "Dockerfile"))
    assert os.path.exists(os.path.join(ROOT, "spark", "Dockerfile"))
    assert os.path.exists(os.path.join(ROOT, "kafka", "Dockerfile"))
    assert os.path.exists(os.path.join(ROOT, "sample_dotnet_backend", "Dockerfile"))


def test_prometheus_config_exists():
    """Verify Prometheus configuration exists."""
    assert os.path.exists(os.path.join(ROOT, "monitoring", "prometheus.yml"))


def test_dbt_medallion_exists():
    """Verify dbt Medallion project structure exists."""
    dbt_dir = os.path.join(ROOT, "dbt_medallion")
    assert os.path.exists(os.path.join(dbt_dir, "dbt_project.yml"))
    assert os.path.exists(os.path.join(dbt_dir, "models", "staging"))
    assert os.path.exists(os.path.join(dbt_dir, "models", "intermediate"))
    assert os.path.exists(os.path.join(dbt_dir, "models", "marts"))


def test_ai_vector_and_agent_modules_exist():
    """Verify AI vector pipeline, agents, and portal files exist."""
    assert os.path.exists(os.path.join(ROOT, "ai_vector_pipeline", "vector_ingestion_engine.py"))
    assert os.path.exists(os.path.join(ROOT, "agents", "nifty50_agent.py"))
    assert os.path.exists(os.path.join(ROOT, "apps", "ai_data_portal", "app.py"))
