FROM python:3.12-slim

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy the application into the container.
COPY . /app

# Install the application dependencies.
WORKDIR /app
RUN uv sync --frozen --no-cache

# Alembic 마이그레이션
RUN uv run alembic upgrade head

# Command to run worker
CMD ["uv", "run", "app/main.py"]