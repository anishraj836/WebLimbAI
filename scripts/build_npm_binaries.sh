#!/usr/bin/env bash
set -euo pipefail

VERSION="${VERSION:-v1.5.0}"
PROJECT_NAME="weblimb"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="${ROOT_DIR}/dist/binaries"

echo "=== Building WebLimbAI NPM Cross-Platform Binaries (${VERSION}) ==="
mkdir -p "${DIST_DIR}"

PLATFORMS=(
  "darwin/arm64"
  "darwin/amd64"
  "linux/amd64"
  "linux/arm64"
  "windows/amd64"
)

rm -rf "${DIST_DIR:?}"/*
CHECKSUM_FILE="${DIST_DIR}/checksums.txt"
> "${CHECKSUM_FILE}"

for PLATFORM in "${PLATFORMS[@]}"; do
  GOOS="${PLATFORM%/*}"
  GOARCH="${PLATFORM#*/}"

  EXT=""
  ARCHIVE_EXT="tar.gz"
  if [ "${GOOS}" = "windows" ]; then
    EXT=".exe"
    ARCHIVE_EXT="zip"
  fi

  BINARY_NAME="${PROJECT_NAME}${EXT}"
  ARCHIVE_NAME="${PROJECT_NAME}_${GOOS}_${GOARCH}.${ARCHIVE_EXT}"
  STAGE_DIR="$(mktemp -d)"

  echo "--> Building ${GOOS}/${GOARCH}..."
  CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o "${STAGE_DIR}/${BINARY_NAME}" \
    "${ROOT_DIR}/cmd/lightlimbs"

  echo "--> Packaging ${ARCHIVE_NAME}..."
  if [ "${ARCHIVE_EXT}" = "zip" ]; then
    if command -v zip >/dev/null 2>&1; then
      (cd "${STAGE_DIR}" && zip -q "${DIST_DIR}/${ARCHIVE_NAME}" "${BINARY_NAME}")
    else
      # Fallback to python / powershell / tar if zip utility is not present
      (cd "${STAGE_DIR}" && python3 -c "import zipfile; z = zipfile.ZipFile('${DIST_DIR}/${ARCHIVE_NAME}', 'w', zipfile.ZIP_DEFLATED); z.write('${BINARY_NAME}'); z.close()")
    fi
  else
    tar -czf "${DIST_DIR}/${ARCHIVE_NAME}" -C "${STAGE_DIR}" "${BINARY_NAME}"
  fi

  rm -rf "${STAGE_DIR}"

  # Calculate SHA-256 checksum
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "${DIST_DIR}" && sha256sum "${ARCHIVE_NAME}" >> "${CHECKSUM_FILE}")
  elif command -v shasum >/dev/null 2>&1; then
    (cd "${DIST_DIR}" && shasum -a 256 "${ARCHIVE_NAME}" >> "${CHECKSUM_FILE}")
  fi
done

echo ""
echo "=== Build Complete ==="
cat "${CHECKSUM_FILE}"
