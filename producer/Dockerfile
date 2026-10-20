FROM python:3.11-slim-bookworm
RUN pip install --no-cache-dir boto3==1.43.115
WORKDIR /app
COPY producer.py .
# Run as a non-root user; AWS credentials are mounted read-only at runtime
RUN useradd -m app
USER app
ENTRYPOINT ["python", "producer.py"]
