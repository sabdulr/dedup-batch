#!/usr/bin/env bash
set -Eeuo pipefail

SOFT_NOFILE_LIMIT="${SOFT_NOFILE_LIMIT:-16384}"

current_soft="$(ulimit -Sn)"
current_hard="$(ulimit -Hn)"

if [[ "${current_soft}" -lt "${SOFT_NOFILE_LIMIT}" ]]; then
  if [[ "${current_hard}" -lt "${SOFT_NOFILE_LIMIT}" ]]; then
    echo "[run] WARN: hard nofile limit (${current_hard}) is below requested soft limit (${SOFT_NOFILE_LIMIT}); using hard limit" >&2
    ulimit -S -n "${current_hard}"
  else
    ulimit -S -n "${SOFT_NOFILE_LIMIT}"
  fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#cd "${SCRIPT_DIR}"

APP_DIR=$(dirname "$SCRIPT_DIR")
cd "${APP_DIR}"


APP_DEBUG="${APP_DEBUG:-false}"
PORT="${PORT:-8080}"
PROFILE="${PROFILE:-}"
CURRENT_TIMESTAMP=$(date +'%Y%m%d%H%M%S%s')
RUN_DATE="${RUN_DATE:-}"
INPUT_DIR="${INPUT_DIR:-}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
WORK_DIR="${WORK_DIR:-}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-3}"
TABLES_FILE="${TABLES_FILE:-}"
RUN_MODE="${RUN_MODE:-rerun}"
RERUN_ID="${RERUN_ID:-}"
OVERWRITE_OUTPUT="${OVERWRITE_OUTPUT:-false}"
SPRING_BATCH_INIT_SCHEMA="${SPRING_BATCH_INIT_SCHEMA:-never}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

if [[ -z "${RUN_DATE}" ]]; then
  echo "[run] ERROR: RUN_DATE is required (example: 2026-03-11)" >&2
  exit 1
fi

if [[ -z "${INPUT_DIR}" ]]; then
  echo "[run] ERROR: INPUT_DIR is required" >&2
  exit 1
fi

if [[ -z "${OUTPUT_DIR}" ]]; then
  echo "[run] ERROR: OUTPUT_DIR is required" >&2
  exit 1
fi

if [[ -z "${WORK_DIR}" ]]; then
  echo "[run] ERROR: WORK_DIR is required" >&2
  exit 1
fi

if ! command -v java >/dev/null 2>&1; then
  echo "[run] ERROR: java is not installed or not on PATH" >&2
  exit 1
fi


SOFT_NOFILE_LIMIT="${SOFT_NOFILE_LIMIT:-16384}"

current_soft="$(ulimit -Sn)"
current_hard="$(ulimit -Hn)"

if [[ "${current_soft}" -lt "${SOFT_NOFILE_LIMIT}" ]]; then
  if [[ "${current_hard}" -lt "${SOFT_NOFILE_LIMIT}" ]]; then
    echo "[run] WARN: hard nofile limit (${current_hard}) is below requested soft limit (${SOFT_NOFILE_LIMIT}); using hard limit" >&2
    ulimit -S -n "${current_hard}"
  else
    ulimit -S -n "${SOFT_NOFILE_LIMIT}"
  fi
fi

JAR_PATH="$(find "${APP_DIR}/build/libs" -maxdepth 1 -type f -name '*.jar' | grep -v plain | sort | tail -n 1 || true)"
if [[ -z "${JAR_PATH}" ]]; then
  echo "[run] ERROR: No jar found under build/libs. Run ./build.sh first." >&2
  exit 1
fi

READY_FILE="${INPUT_DIR%/}/${RUN_DATE}/${RUN_DATE}.ready"
if [[ ! -f "${READY_FILE}" ]]; then
  echo "[run] ERROR: required ready file not found: ${READY_FILE}" >&2
  exit 1
fi

ARGS=(
  "--app.input-dir=${INPUT_DIR}"
  "--app.output-dir=${OUTPUT_DIR}"
  "--app.work-dir=${WORK_DIR}"
  "--BATCH_META_PATH=${WORK_DIR}/batch-meta"
  "--app.run-date=${RUN_DATE}"
  "--app.lookback-days=${LOOKBACK_DAYS}"
  "--app.run-mode=${RUN_MODE}"
  "--app.overwrite-output=${OVERWRITE_OUTPUT}"
  "--spring.batch.jdbc.initialize-schema=${SPRING_BATCH_INIT_SCHEMA}"
  "--server.port=${PORT}"
)

if [[ -n "${PROFILE}" ]]; then
  ARGS+=("--spring.profiles.active=${PROFILE}")
fi

#--logging.level.org.springframework.batch=DEBUG
if [[ "$APP_DEBUG" == "true" ]]; then
  ARGS+=("--logging.level.org.springframework.batch=DEBUG")
  ARGS+=("--logging.level.com.bns.files.dedup=DEBUG")
fi

if [[ -n "${TABLES_FILE}" ]]; then
  ARGS+=("--app.tables-file=${TABLES_FILE}")
fi

if [[ -z "${RERUN_ID}" ]]; then
  RERUN_ID=${CURRENT_TIMESTAMP}
fi

ARGS+=("--app.rerun-id=${RERUN_ID}")

echo "[run] Starting application"
echo "[run] Jar: ${JAR_PATH}"
echo "[run] Run date: ${RUN_DATE}"
echo "[run] Run mode: ${RUN_MODE}"
echo "[run] Run ID: ${RERUN_ID}"
echo "[run] Input dir: ${INPUT_DIR}"
echo "[run] Output dir: ${OUTPUT_DIR}"
echo "[run] Work dir: ${WORK_DIR}"
echo "[run] Ready file: ${READY_FILE}"
echo "-----------------------------------"
echo "Executing: java -jar \"${JAR_PATH}\" \"${ARGS[@]}\" ${EXTRA_ARGS}"

exec java -jar "${JAR_PATH}" "${ARGS[@]}" ${EXTRA_ARGS}

exit $?


