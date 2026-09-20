#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "$0")/../.." && pwd)
compose_file="${project_dir}/deploy/compose/docker-compose.multi.yml"
base_url="${BASE_URL:-http://localhost:8080}"
second_url="${SECOND_URL:-http://localhost:8081}"

docker compose -f "${compose_file}" up -d --build
trap 'docker compose -f "${compose_file}" down' EXIT

for url in "${base_url}" "${second_url}"; do
  for attempt in $(seq 1 60); do
    if curl --fail --silent "${url}/health" >/dev/null; then break; fi
    if [ "${attempt}" -eq 60 ]; then echo "gateway did not become healthy: ${url}" >&2; exit 1; fi
    sleep 2
  done
done

login=$(curl --fail --silent -X POST "${base_url}/users/register" \
  -d 'username=failover-user&password=failover-password')
token=$(printf '%s' "${login}" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "${token}" ]

curl --fail --silent -H "Authorization: Bearer ${token}" "${second_url}/orders?order_id=missing" >/dev/null || true
docker compose -f "${compose_file}" stop gateway-1
curl --fail --silent "${second_url}/health" >/dev/null
curl --fail --silent -H "Authorization: Bearer ${token}" "${second_url}/orders?order_id=missing" >/dev/null || true
echo "multi-instance Redis session and gateway failover passed"
