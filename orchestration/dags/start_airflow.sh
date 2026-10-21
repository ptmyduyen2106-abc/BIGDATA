#!/usr/bin/env bash
set -euo pipefail
mkdir -p /opt/airflow/state/logs
airflow db migrate
if ! airflow users list --output json | python -c 'import json,sys; sys.exit(0 if any(x["username"] == "admin" for x in json.load(sys.stdin)) else 1)'; then
  airflow users create --username admin --password demo-admin --firstname Demo --lastname Admin --role Admin --email admin@example.invalid
fi
airflow scheduler &
SCHEDULER_PID=$!
trap 'kill "$SCHEDULER_PID" 2>/dev/null || true' EXIT INT TERM
airflow webserver --port 8080 &
WEB_PID=$!
wait -n "$SCHEDULER_PID" "$WEB_PID"
kill "$SCHEDULER_PID" "$WEB_PID" 2>/dev/null || true
exit 1
