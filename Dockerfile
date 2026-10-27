FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    RUN_ENV=kubernetes

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# No default server command: different Kubernetes pods run different Python entrypoints.
