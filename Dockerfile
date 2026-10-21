FROM python:3.11.13-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java
ENV JAVA_HOME=/opt/java PYTHONUNBUFFERED=1 PYSPARK_PYTHON=python SPARK_LOCAL_IP=127.0.0.1 OUTPUT_ROOT=/app/outputs
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY requirements-ml.txt .
COPY vendor_wheels /opt/wheels
RUN if find /opt/wheels -name '*.whl' -print -quit | grep -q .; then \
      python -m pip install --no-index --find-links /opt/wheels -r requirements-ml.txt; \
    else python -m pip install --no-cache-dir -r requirements-ml.txt; fi
ENV PYTHONPATH=/app
COPY . /app
CMD ["python", "-m", "pipeline.runner", "--help"]
