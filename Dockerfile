FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl_tasks.py main_flow.py schemas.py seed_data.py ./
COPY source_data/ ./source_data/

CMD ["python", "main_flow.py"]
