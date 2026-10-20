# Local test runner: same Spark version as the Databricks serverless workspace (4.2.0)
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==4.2.0 pytest==9.1.1 boto3==1.43.115
WORKDIR /lab
ENTRYPOINT ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"]
