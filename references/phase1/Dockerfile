FROM python:3.11.13-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java
ENV JAVA_HOME=/opt/java PYTHONUNBUFFERED=1 PYSPARK_PYTHON=python
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts /app/scripts
