FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["sh", "/app/entrypoint.sh"]
CMD ["sh", "-c", "python /app/run_dbt.py run --profiles-dir /app && python /app/run_dbt.py test --profiles-dir /app"]
