FROM python:3.13-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY script.py .
ENTRYPOINT ['python','script.py']
