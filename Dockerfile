# syntax=docker/dockerfile:1.7
#
# Two-stage Alpine build. Flask + requests are pure Python, so musl costs
# nothing and drops the Debian-slim CVE backlog (perl-base, ncurses, etc.).
# Python raised 3.9 -> 3.13; 3.9 went end-of-life in October 2025.

# ---- Stage 1: build the virtualenv ----
FROM python:3.13-alpine AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apk add --no-cache build-base libffi-dev openssl-dev

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Stage 2: runtime ----
FROM python:3.13-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

RUN apk add --no-cache libffi openssl

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

# Copy the required files
COPY app.py .
COPY background_scraper.py .

# Set environment variables (including the API key for basic authentication) API KEY IS NOT REAL
ENV API_KEY=499817f8-623f-4fae-b828-8dc551aba9bb
ENV DB_PATH=/data/SermonProcessor.db
ENV AUDIO_DIR=/data/audiofiles

# Non-root runtime. /data holds the SQLite database and the downloaded audio.
RUN addgroup -g 1000 -S appuser \
    && adduser -u 1000 -S -G appuser -H -s /sbin/nologin appuser \
    && mkdir -p /data/audiofiles \
    && chown -R appuser:appuser /app /data

USER 1000:1000

EXPOSE 5060

CMD ["python", "app.py"]
