FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.2.1

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

COPY app/ ./app/
COPY migration_project.py run_deploy_migrations.py ./
COPY migrations/ ./migrations/

EXPOSE 8001

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
