#!/usr/bin/env bash
# Upload the PySpark job to GCS so Dataproc Serverless can read it.
#
# Usage:
#   ./spark/deploy.sh
#
# Reads GCS_BUCKET from the environment (set via `.env`). The destination path
# is the one the Kestra flow and ad-hoc `make submit-spark-batch` expect.

set -euo pipefail

: "${GCS_BUCKET:?GCS_BUCKET must be set (source .env first)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$SCRIPT_DIR/jobs/weather_aggregations.py"
DEST="gs://${GCS_BUCKET}/code/spark/weather_aggregations.py"

if [[ ! -f "$SRC" ]]; then
  echo "ERROR: job file not found at $SRC" >&2
  exit 1
fi

echo "Uploading $SRC → $DEST"
gcloud storage cp "$SRC" "$DEST"
echo "✅ Spark job deployed."
echo "   Next: submit with 'make submit-spark-batch' or wait for the Kestra schedule."
