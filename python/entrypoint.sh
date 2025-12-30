#!/bin/bash
set -e

echo "Waiting for MySQL to be ready..."

until mysqladmin ping -h"$DB_HOST" --silent; do
  echo "MySQL not ready yet..."
  sleep 5
done

echo "MySQL is ready. Starting fetch loop..."

while true
do
  echo "Fetching JSM data..."
  python fetch_jsm.py || true

  echo "Fetching JIRA data..."
  python fetch_jira.py || true

  echo "Sleeping for 5 minutes..."
  sleep 300
done