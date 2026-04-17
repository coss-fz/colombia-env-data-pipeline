# Slim python base — we don't need a full OS, pandas/pyarrow wheels are self-contained.
FROM python:3.11-slim

WORKDIR /app

# System deps: ca-certs so HTTPS works and gcc only if we need to build wheels (we don't,
# but a tiny belt-and-braces install keeps this robust to dependency upgrades).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source last so code changes don't bust the pip cache layer.
COPY utils/ ./utils/
COPY weather/ ./weather/
COPY air_quality/ ./air_quality/

# Make `python -m weather.fetch_weather` discoverable from /app without packaging overhead.
ENV PYTHONPATH=/app

# Default entry is a shell — Kestra and ad-hoc runs pick the specific module at invocation time.
CMD ["bash"]
