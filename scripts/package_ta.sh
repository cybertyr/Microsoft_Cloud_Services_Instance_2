#!/usr/bin/env bash
# Build a deployable tarball whose top-level folder is the correct Splunk app name.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="Splunk_TA_microsoft-cloudservices_instance_2"
SRC="${REPO_ROOT}/${APP_NAME}"
OUT_DIR="${REPO_ROOT}/dist"
STAMP="$(date +%Y%m%d)"
OUT="${OUT_DIR}/${APP_NAME}-${STAMP}.tgz"

if [[ ! -f "${SRC}/default/app.conf" ]]; then
  echo "ERROR: missing ${SRC}/default/app.conf" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"
tar -C "${REPO_ROOT}" -czf "${OUT}" "${APP_NAME}"
echo "Created ${OUT}"
echo "Install with:"
echo "  tar -xzf ${OUT} -C \"\$SPLUNK_HOME/etc/apps/\""
echo "  # → \$SPLUNK_HOME/etc/apps/${APP_NAME}/"
