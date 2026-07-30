# syntax=docker/dockerfile:1.7
#
# One-shot scraper. Two-stage Alpine build: `requests` is pure Python, so the
# runtime stage needs no compilers and no Debian package surface. The previous
# curl/unzip apt install was unused by sermon_scraper.py and has been dropped.
# Python raised 3.9 -> 3.13; 3.9 went end-of-life in October 2025.

# ---- Stage 1: build the virtualenv ----
FROM python:3.13-alpine AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apk add --no-cache build-base libffi-dev openssl-dev

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Stage 2: runtime ----
FROM python:3.13-alpine

# Ensure Python can log in real-time
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

RUN apk add --no-cache libffi openssl

# Create working directory
WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

# Copy scraper script
COPY sermon_scraper.py .

# Non-root runtime; /data is where the scraper writes its output.
RUN addgroup -g 1000 -S appuser \
    && adduser -u 1000 -S -G appuser -H -s /sbin/nologin appuser \
    && mkdir -p /data \
    && chown -R appuser:appuser /app /data

USER 1000:1000

# Run the script
CMD ["python", "sermon_scraper.py"]
