#!/bin/sh
set -e

echo "Starting Jira Fetcher..."

while true; do
  echo "Running JSM fetch..."
  python fetch_jsm.py || echo "JSM fetch failed"

  echo "Running JIRA fetch..."
  python fetch_jira.py || echo "JIRA fetch failed"

  echo "Sleeping for 5 minutes..."
  sleep 300
done