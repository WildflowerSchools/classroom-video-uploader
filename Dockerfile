FROM python:3.10.11-slim

WORKDIR /app

RUN apt-get update -y && \
    apt-get install -y ffmpeg libsm6 libxext6 && \
    pip install --upgrade pip opencv-contrib-python poetry wheel

COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false && poetry install --only main --no-root --no-interaction --no-ansi

COPY ./uploader /app/uploader
