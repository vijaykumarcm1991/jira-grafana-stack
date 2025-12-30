#!/bin/bash

echo "Starting Jira Fetcher..."

while true
do
  echo "Fetching JSM data..."
  python fetch_jsm.py

  echo "Fetching JIRA data..."
  python fetch_jira.py

  echo "Sleeping for 5 minutes..."
  sleep 300
done