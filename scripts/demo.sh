#!/usr/bin/env bash
# End-to-end demo of the inference API. Run it while the stack is up:
#   docker compose up --build -d && ./scripts/demo.sh
# Handy for taking the screenshots required in the README (see docs/screenshots/).
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:${FASTAPI_PORT:-8000}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"

step() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
pretty() { python3 -m json.tool 2>/dev/null || cat; }
field() { python3 -c "import json,sys; print(json.load(sys.stdin)['$1'])"; }

wait_until_finished() {
  local id="$1" deadline=$((SECONDS + TIMEOUT_SECONDS)) body status
  while (( SECONDS < deadline )); do
    body="$(curl -s "$BASE_URL/inference/$id")"
    status="$(printf '%s' "$body" | field status)"
    printf '   status=%s\n' "$status" >&2
    if [[ "$status" == "COMPLETED" || "$status" == "FAILED" ]]; then
      printf '%s' "$body"
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for $id" >&2
  return 1
}

step "1. Health check  (GET /health)"
curl -s -i "$BASE_URL/health" | sed -n '1p;$p'

step "2. Submit an inference request  (POST /inference -> 202 Accepted)"
RESPONSE="$(curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE_URL/inference" \
  -H 'Content-Type: application/json' \
  -d '{"input_data": "A photo of a cat on a sofa", "model_id": "resnet50"}')"
printf '%s\n' "$RESPONSE"
REQUEST_ID="$(printf '%s' "$RESPONSE" | head -n1 | field request_id)"

step "3. Poll the result while the consumer works  (GET /inference/$REQUEST_ID)"
wait_until_finished "$REQUEST_ID" | pretty

step "4. Unknown request id  (-> 404 Not Found)"
curl -s -w '\nHTTP %{http_code}\n' "$BASE_URL/inference/does-not-exist"

step "5. Invalid payload  (-> 422 Unprocessable Entity)"
curl -s -w '\nHTTP %{http_code}\n' -X POST "$BASE_URL/inference" \
  -H 'Content-Type: application/json' -d '{"input_data": ""}'

step "6. Failing inference  (consumer marks the request FAILED)"
FAIL_ID="$(curl -s -X POST "$BASE_URL/inference" -H 'Content-Type: application/json' \
  -d '{"input_data": "please __fail__ this one"}' | field request_id)"
wait_until_finished "$FAIL_ID" | pretty

step "Done. Inspect the structured logs with:  docker compose logs --no-log-prefix api_gateway kafka_consumer"
