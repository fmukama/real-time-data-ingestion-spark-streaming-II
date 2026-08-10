FROM quay.io/jupyter/pyspark-notebook:2026-08-08

# ---------------------------------------------------------------------------
# Spark on PATH/PYTHONPATH for EVERY process, not just notebook kernels.
#
# The base image makes `import pyspark` work via
# /usr/local/bin/before-notebook.d/10spark-config.sh, which sets PYTHONPATH --
# but that hook only runs as part of the container's entrypoint sequence, so a
# `docker compose exec` session (which bypasses the entrypoint) never sees it.
# Since `make test`, `make generate` and `make stream` all run via exec, baking
# these in as ENV is what makes them work at all. Found the hard way in the
# sibling project, where it silently broke pytest.
# ---------------------------------------------------------------------------
ENV SPARK_HOME=/usr/local/spark

USER root

# The py4j zip is version-stamped (py4j-0.10.9.9-src.zip today) and that stamp
# changes whenever the base image's Spark does. Rather than hardcode it into
# PYTHONPATH -- a silent breakage waiting for the next image bump -- resolve it
# once at build time behind a stable symlink. `set -eu` + the explicit test mean
# a missing/renamed zip fails the BUILD loudly instead of producing a container
# where `import pyspark` mysteriously doesn't work.
RUN set -eu; \
    py4j="$(ls "${SPARK_HOME}"/python/lib/py4j-*-src.zip | head -1)"; \
    test -f "$py4j"; \
    ln -sf "$py4j" "${SPARK_HOME}/python/lib/py4j-src.zip"; \
    echo "Linked py4j-src.zip -> $py4j"

ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-src.zip
ENV PATH=${SPARK_HOME}/bin:${PATH}

# ---------------------------------------------------------------------------
# PostgreSQL JDBC driver, baked in at a pinned version.
#
# The alternative -- `--packages org.postgresql:postgresql:...` at submit time --
# re-resolves from Maven Central on EVERY run: slow startup, needs network, and
# it fails the day you demo offline. Baking it makes the build deterministic.
#
# Note this is the JVM-side driver. `psycopg2` (in requirements.txt) is the
# Python-side client used for the ON CONFLICT upsert. Two different
# worlds; the project genuinely needs both.
# ---------------------------------------------------------------------------
ENV POSTGRES_JDBC_VERSION=42.7.13
ENV POSTGRES_JDBC_JAR=/opt/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

RUN set -eu; \
    mkdir -p /opt/jars; \
    curl -fsSL -o "${POSTGRES_JDBC_JAR}" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"; \
    test -s "${POSTGRES_JDBC_JAR}"; \
    chmod 644 "${POSTGRES_JDBC_JAR}"

# Checkpoints live in a named volume, NOT the Windows bind mount -- checkpointing
# relies on atomic-rename semantics that bind mounts don't reliably honour
# Creating the mountpoint here with the notebook user's
# ownership matters: when Docker first populates a named volume from the image,
# it carries this directory's ownership across. Without it the volume lands
# root-owned and Spark can't write its offsets.
RUN mkdir -p /opt/checkpoints && chown "${NB_UID}:${NB_GID}" /opt/checkpoints

USER ${NB_UID}

WORKDIR /home/jovyan/work

COPY --chown=${NB_UID}:${NB_GID} requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
