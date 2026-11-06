#!/bin/sh
set -eu
ADDRESS="${TEMPORAL_ADDRESS:-temporal:7233}"
for i in $(seq 1 30); do
  if temporal operator cluster health --address "$ADDRESS" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
if temporal operator namespace describe -n default --address "$ADDRESS" >/dev/null 2>&1; then
  echo 'Namespace default already exists'
else
  temporal operator namespace create -n default --address "$ADDRESS" --retention 7d
fi
