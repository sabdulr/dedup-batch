#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#cd "${SCRIPT_DIR}"

APP_DIR=$(dirname "$SCRIPT_DIR")
cd "${APP_DIR}"
#echo "APP_DIR: ${APP_DIR}"


echo "[build] Starting build in: ${APP_DIR}"

if ! command -v java >/dev/null 2>&1; then
  echo "[build] ERROR: java is not installed or not on PATH" >&2
  exit 1
fi

if ! command -v ./gradlew >/dev/null 2>&1; then
  :
fi

if [[ ! -f "${APP_DIR}/gradlew" ]]; then
  echo "[build] ERROR: gradlew not found in project root" >&2
  exit 1
fi

chmod +x ${APP_DIR}/gradlew

echo "[build] Java version:"
java -version

echo "[build] Cleaning previous outputs"
 ${APP_DIR}/gradlew clean

echo "[build] Compiling and running tests"
 ${APP_DIR}/gradlew test

#echo "[build] Build (without tests)"
#./gradlew build -x test

echo "[build] Building boot jar"
${APP_DIR}/gradlew bootJar

JAR_PATH="$(find "${APP_DIR}/build/libs" -maxdepth 1 -type f -name '*.jar' | sort | tail -n 1 || true)"

if [[ -z "${JAR_PATH}" ]]; then
  echo "[build] ERROR: No jar found under ${APP_DIR}/build/libs" >&2
  exit 1
fi

echo "[build] Build successful"
echo "[build] Jar created at: ${JAR_PATH}"
