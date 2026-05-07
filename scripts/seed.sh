#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"

curl -fsS -X POST "$API_URL/v1/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","handler":"echo","payload":{"msg":"hello from seed"}}'
curl -fsS -X POST "$API_URL/v1/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","handler":"sleep","payload":{"duration":"500ms"}}'
curl -fsS -X POST "$API_URL/v1/jobs" -H 'content-type: application/json' \
  -d '{"queue":"default","handler":"fail","payload":{"message":"seeded failure"},"max_attempts":1}'
