# Official Spark image with Python
FROM apache/spark:4.1.1-python3

# Alternate to root user for privilege
USER root

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Go to work directory
WORKDIR /app

# Copy files necessary for installing dependencies first
COPY pyproject.toml uv.lock ./

ENV UV_PYTHON=python3.12

# Install dependencies using uv
# --frozen make sure to use the exact versions in uv.lock
# --no-install-project no install of the project itself, only dependencies
RUN uv sync --frozen --no-install-project

# Download PostgreSQL JDBC driver and place it in Spark's jars directory
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/bitnami/spark/jars/
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/spark/jars/

# Copy all project files into the container
COPY . .

# Use the virtual environment for all subsequent commands
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Env variables for PySpark to use the correct Python interpreter
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Standard command to show help, can be overridden when running the container
CMD ["python3", "jobs/ingest_bronze.py", "--help"]