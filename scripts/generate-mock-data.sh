#!/usr/bin/env bash
set -Eeuo pipefail

# time ./generate-mock-data.sh award 2026-03-11 50000 .


if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "Usage: $0 <table-name> <yyyy-mm-dd> [total-record-count] [output-dir]" >&2
  exit 1
fi

TABLE="$1"
RUN_DATE="$2"
TOTAL_RECORDS="${3:-20000}"
OUTPUT_DIR="${4:-.}"
#RECORDS_PER_CHUNK=200000
RECORDS_PER_CHUNK="${5:-20000}"
DUP_AT="${6:-20}"

if ! [[ "${RUN_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: date must be in yyyy-mm-dd format" >&2
  exit 1
fi

if ! date -d "${RUN_DATE}" "+%Y-%m-%d" >/dev/null 2>&1; then
  echo "ERROR: invalid date '${RUN_DATE}'" >&2
  exit 1
fi

if ! [[ "${TOTAL_RECORDS}" =~ ^[0-9]+$ ]] || [[ "${TOTAL_RECORDS}" -le 0 ]]; then
  echo "ERROR: total-record-count must be a positive integer" >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

chunk_index=1
record_in_chunk=0
total_written=0
current_file=""

open_new_chunk() {
  current_file="${OUTPUT_DIR}/${TABLE}-${RUN_DATE}-${chunk_index}.json"
  : > "${current_file}"
}

json_escape() {
  local s="${1//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '%s' "${s}"
}

pad12() {
  printf "%012d" "$1"
}

generate_timestamp() {
  local n="$1"
  local hour=$(( n % 24 ))
  local minute=$(( (n / 24) % 60 ))
  local second=$(( (n / 1440) % 60 ))
  printf "%sT%02d:%02d:%02d.00Z" "${RUN_DATE}" "${hour}" "${minute}" "${second}"
}

write_record() {
  local id="$1"
  local ts="$2"
  local account_num="$3"

  printf '{"Source":{"Table":"%s","System":"CORE"},"ChangeData":{"id":"%s","ChangeDateTime":"%s","Operation":"UPSERT"},"Data":{"_creationdate":"%s","accountId":"A%s","status":"OPEN","balanceBand":"HIGH"}}\n' \
    "$(json_escape "${TABLE}")" \
    "${id}" \
    "${ts}" \
    "${ts}" \
    "${account_num}" >> "${current_file}"
}

open_new_chunk

for ((i=1; i<=TOTAL_RECORDS; i++)); do
  if (( record_in_chunk == RECORDS_PER_CHUNK )); then
    chunk_index=$((chunk_index + 1))
    record_in_chunk=0
    open_new_chunk
  fi

  if (( i % DUP_AT == 0 )); then
    source_index=$(( i - 1 ))
  else
    source_index=$i
  fi

  id="$(pad12 "${source_index}")"
  ts="$(generate_timestamp "${source_index}")"
  account_num="$(printf '%03d' $(( (source_index % 900) + 100 )))"

  write_record "${id}" "${ts}" "${account_num}"

  record_in_chunk=$((record_in_chunk + 1))
  total_written=$((total_written + 1))
done

echo "Generated ${total_written} records for table '${TABLE}' in ${chunk_index} file(s) under ${OUTPUT_DIR}"
