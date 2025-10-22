FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data

EXPOSE 10000
ENV PORT=10000

CMD gunicorn --bind 0.0.0.0:$PORT \
    --timeout 1800 \
    --workers 1 \
    --threads 1 \
    --log-level info \
    scraper_api:app