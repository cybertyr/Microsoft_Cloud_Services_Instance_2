#!/usr/bin/env bash
# Install ONLY the Instance 2 TA into Splunk etc/apps (never the GitHub repo root).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_NAME="Splunk_TA_microsoft-cloudservices_instance_2"
SRC="${REPO_ROOT}/${APP_NAME}"

if [[ ! -f "${SRC}/default/app.conf" ]]; then
  echo "ERROR: missing ${SRC}/default/app.conf — refuse to install." >&2
  exit 1
fi

SPLUNK_HOME="${SPLUNK_HOME:-}"
if [[ -z "${SPLUNK_HOME}" ]]; then
  if [[ -d /opt/splunk ]]; then
    SPLUNK_HOME=/opt/splunk
  else
    echo "ERROR: set SPLUNK_HOME (e.g. export SPLUNK_HOME=/opt/splunk)" >&2
    exit 1
  fi
fi

DEST="${SPLUNK_HOME}/etc/apps/${APP_NAME}"
echo "Installing ${APP_NAME}"
echo "  from: ${SRC}"
echo "  to:   ${DEST}"

# Warn about common wrong installs (GitHub Code → Download ZIP)
shopt -s nullglob
for bad in \
  "${SPLUNK_HOME}/etc/apps/Microsoft_Cloud_Services_Instance_2-main" \
  "${SPLUNK_HOME}/etc/apps/microsoft_cloud_services_instance_2-main" \
  "${SPLUNK_HOME}/etc/apps/Microsoft_Cloud_Services_Instance_2" \
  "${SPLUNK_HOME}/etc/apps/microsoft_cloud_services_instance_2"
do
  if [[ -d "${bad}" ]]; then
    echo "WARNING: found likely wrong install folder:" >&2
    echo "  ${bad}" >&2
    echo "  That folder is the GitHub repo root, not the TA. Remove it after verifying," >&2
    echo "  or Splunk will keep showing generic Search navigation." >&2
  fi
done
shopt -u nullglob

mkdir -p "${SPLUNK_HOME}/etc/apps"
rm -rf "${DEST}"
cp -a "${SRC}" "${DEST}"

echo "OK. Next:"
echo "  1) Restart Splunk (or at least Splunk Web)"
echo "  2) Open app: ${APP_NAME}"
echo "  3) Expect tabs: Inputs / Configuration / Search / Health Dashboard"
echo "  4) URL should contain /app/${APP_NAME}/inputs  (not ...-main/search)"
