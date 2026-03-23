#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="${1:-../work/rocksdb}"
RETENTION_DAYS="${2:-10}"
CUTOFF="$(date -d "-${RETENTION_DAYS} days" +%F)"

find "${ROOT}" -mindepth 1 -maxdepth 1 -type d | while read -r dir; do
  name="$(basename "${dir}")"
  if [[ "${name}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && [[ "${name}" < "${CUTOFF}" ]]; then
    echo "Deleting ${dir}"
    rm -rf -- "${dir}"
  fi
done
