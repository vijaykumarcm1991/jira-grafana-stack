#!/bin/sh
set -e

echo "Starting Jira Fetcher..."

while true; do
  echo "Running JSM fetch..."
  python fetch_jsm.py || echo "JSM fetch failed"

  echo "Running JIRA fetch..."
  python fetch_jira.py || echo "JIRA fetch failed"

  echo "Sleeping for 60 Seconds..."
  sleep 60
done