#!/bin/bash
set -e

echo "Waiting for MySQL to be ready..."

until mysqladmin ping -h"$DB_HOST" --silent; do
  echo "MySQL not ready yet..."
  sleep 5
done

echo "MySQL is ready. Starting Jira fetch loop..."

while true
do
  echo "Fetching JSM data..."
  python fetch_jsm.py || echo "JSM fetch failed"

  echo "Fetching JIRA data..."
  python fetch_jira.py || echo "JIRA fetch failed"

  echo "Sleeping for 5 minutes..."
  sleep 300
done