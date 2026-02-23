# Official Spark image with Python 3.9
FROM bitnami/spark:3.5

# Alternate to root user for privilege
USER root

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Go to work directory
WORKDIR /app

# Copy files necessary for installing dependencies first
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
# --frozen make sure to use the exact versions in uv.lock
# --no-install-project no install of the project itself, only dependencies
RUN uv sync --frozen --no-install-project

# Download PostgreSQL JDBC driver and place it in Spark's jars directory
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/bitnami/spark/jars/

# Copy all project files into the container
COPY . .

# Garante que o Python use o ambiente virtual criado pelo uv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Define variáveis de ambiente para o Spark usar o Python correto
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Standard command to show help, can be overridden when running the container
CMD ["python3", "jobs/ingest_bronze.py", "--help"]