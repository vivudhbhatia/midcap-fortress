#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION:-us-central1}"
PROJECT_ID="$(gcloud config get-value project)"
REPO="rr-docker"

SERVICE_NAME="regreport-api"
JOB_NAME="rr-agent-runner"
WORKFLOW_NAME="rr-pipeline"

INSTANCE_NAME="rr-postgres"
DB_NAME="regreport"
DB_USER="rr_app"
SECRET_NAME="rr-database-url"

RAW_BUCKET="rr-raw-sources-${PROJECT_ID}"
PROC_BUCKET="rr-processed-${PROJECT_ID}"
EXP_BUCKET="rr-exports-${PROJECT_ID}"

API_SA="rr-api-sa@${PROJECT_ID}.iam.gserviceaccount.com"
AGENT_SA="rr-agent-sa@${PROJECT_ID}.iam.gserviceaccount.com"
WF_SA="rr-workflows-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "============================================================"
echo "Project: $PROJECT_ID"
echo "Region : $REGION"
echo "============================================================"

# Must be run from repo root
test -f services/api/main.py
test -f workflows/rr_pipeline.yaml

echo "1) Patch API health endpoint: /healthz -> /health"
python3 - <<'PY'
from pathlib import Path

p = Path("services/api/main.py")
s = p.read_text()

# Cloud Run reserves some paths ending in 'z' (e.g., /healthz), so use /health instead.
changed = False

if '@app.get("/healthz")' in s and '@app.get("/health")' not in s:
    s = s.replace('@app.get("/healthz")', '@app.get("/health")')
    changed = True

# Rename the function for clarity (optional)
s2 = s.replace("def healthz()", "def health()")
if s2 != s:
    s = s2
    changed = True

if changed:
    p.write_text(s)
    print("✅ Patched services/api/main.py to use /health")
else:
    print("✅ No patch needed (already using /health or already patched).")
PY

echo "2) Build a NEW API image tag (avoids cache/overwrite confusion)"
TAG="0.1.1-$(date +%Y%m%d%H%M%S)"
IMAGE_API="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/regreport-api:${TAG}"
echo "IMAGE_API=$IMAGE_API"

gcloud builds submit --tag "$IMAGE_API" services/api

echo "3) Ensure buckets exist (idempotent)"
for B in "$RAW_BUCKET" "$PROC_BUCKET" "$EXP_BUCKET"; do
  if ! gcloud storage buckets describe "gs://$B" >/dev/null 2>&1; then
    echo "Creating bucket: gs://$B"
    gcloud storage buckets create "gs://$B" --location="$REGION" --uniform-bucket-level-access >/dev/null
  fi
done

echo "4) Deploy workflow (idempotent)"
gcloud workflows deploy "$WORKFLOW_NAME" \
  --source workflows/rr_pipeline.yaml \
  --location "$REGION" \
  --service-account "$WF_SA" \
  --set-env-vars RR_AGENT_JOB_NAME="$JOB_NAME" >/dev/null

INSTANCE_CONN="$(gcloud sql instances describe "$INSTANCE_NAME" --format='value(connectionName)')"
echo "INSTANCE_CONN=$INSTANCE_CONN"

echo "5) Sync DB user password + Secret Manager DSN (deterministic, avoids auth mismatch)"
# URL-safe password
if command -v openssl >/dev/null 2>&1; then
  NEW_PW="$(openssl rand -hex 16)"
else
  NEW_PW="$(python3 - <<'PY'
import secrets
print(secrets.token_hex(16))
PY
)"
fi

gcloud sql users set-password "$DB_USER" \
  --instance "$INSTANCE_NAME" \
  --password "$NEW_PW" >/dev/null

DSN="postgresql://${DB_USER}:${NEW_PW}@/${DB_NAME}?host=/cloudsql/${INSTANCE_CONN}"

if gcloud secrets describe "$SECRET_NAME" >/dev/null 2>&1; then
  printf '%s' "$DSN" | gcloud secrets versions add "$SECRET_NAME" --data-file=- >/dev/null
else
  printf '%s' "$DSN" | gcloud secrets create "$SECRET_NAME" --data-file=- >/dev/null
fi
echo "✅ Secret updated: $SECRET_NAME"

echo "6) Update Cloud Run Job (keep current image, just re-assert DB + CloudSQL + bucket)"
CURRENT_JOB_IMAGE="$(gcloud run jobs describe "$JOB_NAME" --region "$REGION" --format='value(spec.template.template.containers[0].image)')"

gcloud run jobs update "$JOB_NAME" \
  --image "$CURRENT_JOB_IMAGE" \
  --region "$REGION" \
  --service-account "$AGENT_SA" \
  --set-cloudsql-instances "$INSTANCE_CONN" \
  --set-secrets DATABASE_URL="${SECRET_NAME}:latest" \
  --set-env-vars EXPORT_BUCKET="$EXP_BUCKET" >/dev/null

echo "7) Deploy Cloud Run API service"
gcloud run deploy "$SERVICE_NAME" \
  --image "$IMAGE_API" \
  --region "$REGION" \
  --allow-unauthenticated \
  --service-account "$API_SA" \
  --add-cloudsql-instances "$INSTANCE_CONN" \
  --set-secrets DATABASE_URL="${SECRET_NAME}:latest" \
  --set-env-vars WORKFLOW_NAME="$WORKFLOW_NAME",WORKFLOW_LOCATION="$REGION",GOOGLE_CLOUD_PROJECT="$PROJECT_ID" \
  --ingress all >/dev/null

API_URL="$(gcloud run services describe "$SERVICE_NAME" --region "$REGION" --format='value(status.url)')"
echo "API_URL=$API_URL"

echo "8) Verify /health (NOT /healthz)"
curl -sS -i "$API_URL/health" | head -n 20

echo "9) Upload demo docs to RAW bucket"
mkdir -p demo
echo "demo form" > demo/form.pdf
echo "demo instructions" > demo/instructions.pdf

gcloud storage cp demo/form.pdf "gs://$RAW_BUCKET/demo/form.pdf" >/dev/null
gcloud storage cp demo/instructions.pdf "gs://$RAW_BUCKET/demo/instructions.pdf" >/dev/null

SHA_FORM="$(sha256sum demo/form.pdf | awk '{print $1}')"
SHA_INST="$(sha256sum demo/instructions.pdf | awk '{print $1}')"

echo "10) Create run"
RUN_JSON="$(curl -sS -X POST "$API_URL/v1/runs" \
  -H 'Content-Type: application/json' \
  -d "{
    \"report_code\": \"FRY9C\",
    \"effective_date\": \"2025-12-31\",
    \"documents\": [
      {\"gcs_uri\": \"gs://$RAW_BUCKET/demo/form.pdf\", \"doc_type\": \"form\", \"sha256\": \"$SHA_FORM\", \"metadata\": {}},
      {\"gcs_uri\": \"gs://$RAW_BUCKET/demo/instructions.pdf\", \"doc_type\": \"instructions\", \"sha256\": \"$SHA_INST\", \"metadata\": {}}
    ]
  }")"

echo "$RUN_JSON" | jq .

RUN_ID="$(echo "$RUN_JSON" | jq -r .run_id)"
if [[ -z "$RUN_ID" || "$RUN_ID" == "null" ]]; then
  echo "ERROR: No run_id returned. Dumping API logs..."
  gcloud run services logs read "$SERVICE_NAME" --region "$REGION" --limit 200 || true
  exit 1
fi
echo "RUN_ID=$RUN_ID"

echo "11) Poll until DONE_A8_EXPORT"
for i in {1..180}; do
  STATUS="$(curl -sS "$API_URL/v1/runs/$RUN_ID" | jq -r .status)"
  echo "[$i] status=$STATUS"

  if [[ "$STATUS" == "DONE_A8_EXPORT" ]]; then
    echo "✅ Pipeline completed"
    break
  fi

  if [[ "$STATUS" == FAILED* ]]; then
    echo "❌ Pipeline FAILED"
    curl -sS "$API_URL/v1/runs/$RUN_ID" | jq .
    echo "Recent API logs:"
    gcloud run services logs read "$SERVICE_NAME" --region "$REGION" --limit 200 || true
    echo "Recent workflow executions:"
    gcloud workflows executions list --workflow "$WORKFLOW_NAME" --location "$REGION" --limit 5 || true
    echo "Recent job executions:"
    gcloud run jobs executions list --job "$JOB_NAME" --region "$REGION" || true
    exit 1
  fi

  sleep 5
done

echo "12) Verify exports"
curl -sS "$API_URL/v1/runs/$RUN_ID/exports" | jq .

echo "Export summary.json:"
gcloud storage cat "gs://$EXP_BUCKET/runs/$RUN_ID/summary.json" | jq .

echo "============================================================"
echo "E2E SUCCESS ✅ run_id=$RUN_ID"
echo "NOTE: /healthz may still 404 on Cloud Run; use /health going forward."
echo "============================================================"
