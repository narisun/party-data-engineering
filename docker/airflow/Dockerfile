FROM apache/airflow:2.8.2-python3.9
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ---- OS deps + Spark client ----
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates; \
    curl -fL "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz" -o /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    ln -sfn /opt/spark-3.5.1-bin-hadoop3 /opt/spark; \
    rm -f /tmp/spark.tgz; \
    mkdir -p /usr/lib/jvm; \
    JAVA_BIN="$(readlink -f "$(command -v java)")"; \
    ln -sfn "$(dirname "$(dirname "$JAVA_BIN")")" /usr/lib/jvm/java-17-openjdk; \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:/home/airflow/.local/bin:${PATH}"
ENV PYTHONNOUSERSITE=0

# ---- Python deps (driver) + wheels (executors) ----
USER airflow
ARG AIRFLOW_VERSION=2.8.2
ARG PYTHON_VERSION=3.9
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN set -eux; \
    # Airflow-provider + PySpark
    python -m pip install --upgrade --no-cache-dir --user pip; \
    python -m pip install --no-cache-dir --user --constraint "${CONSTRAINTS_URL}" apache-airflow-providers-apache-spark; \
    python -m pip install --no-cache-dir --user pyspark==3.5.1; \
    \
    # Install GE + ruamel in the driver env
    python -m pip install --no-cache-dir --user \
        great_expectations==0.18.9 \
        "ruamel.yaml>=0.17,<0.18" \
        "ruamel.yaml.clib>=0.2,<0.3"; \
    \
    # Also download wheels to ship to executors via --py-files
    mkdir -p /opt/airflow/wheels; \
    python -m pip download --no-deps --dest /opt/airflow/wheels \
        great_expectations==0.18.9 \
        "ruamel.yaml>=0.17,<0.18" \
        "ruamel.yaml.clib>=0.2,<0.3"; \
    cd /opt/airflow/wheels; \
    ln -sfn "$(ls -1 great_expectations-0.18.9-*.whl | head -n1)" great_expectations.whl; \
    ln -sfn "$(ls -1 ruamel.yaml-0.17.*.whl     | head -n1)" ruamel.yaml.whl; \
    ln -sfn "$(ls -1 ruamel.yaml.clib-0.2.*.whl | head -n1)" ruamel.yaml.clib.whl

# (Optional) sanity check during build
RUN python - <<'PY'
from ruamel import yaml
import great_expectations as gx, importlib.util as u, pyspark, os
print("pyspark:", pyspark.__version__)
print("GE:", gx.__version__, "ruamel.yaml:", getattr(yaml, '__version__', 'unknown'))
assert u.find_spec("airflow.providers.apache.spark"), "spark provider missing"
for n in ["great_expectations.whl","ruamel.yaml.whl","ruamel.yaml.clib.whl"]:
    assert os.path.islink(f"/opt/airflow/wheels/{n}"), f"missing {n}"
PY
