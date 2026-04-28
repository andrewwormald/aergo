#!/usr/bin/env bash
set -euo pipefail

# Build the Aeron C client library (libaeron.so/dylib) and media driver (aeronmd)
# from source. Output goes to ./build/aeron/

AERON_VERSION="${AERON_VERSION:-1.46.7}"
BUILD_DIR="${BUILD_DIR:-$(pwd)/build}"
AERON_SRC="${BUILD_DIR}/aeron-src"
AERON_BUILD="${BUILD_DIR}/aeron-build"
AERON_OUT="${BUILD_DIR}/aeron"

echo "=== Building Aeron C client v${AERON_VERSION} ==="
echo "  Source:  ${AERON_SRC}"
echo "  Build:   ${AERON_BUILD}"
echo "  Output:  ${AERON_OUT}"

# Clone if not present
if [ ! -d "${AERON_SRC}" ]; then
    echo "--- Cloning aeron ${AERON_VERSION}..."
    git clone --depth 1 --branch "${AERON_VERSION}" \
        https://github.com/aeron-io/aeron.git "${AERON_SRC}"
else
    echo "--- Using existing source at ${AERON_SRC}"
fi

# Build
mkdir -p "${AERON_BUILD}"
cd "${AERON_BUILD}"

echo "--- Running cmake..."
cmake "${AERON_SRC}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_AERON_DRIVER=ON \
    -DBUILD_AERON_ARCHIVE_API=OFF \
    -DAERON_TESTS=OFF \
    -DAERON_BUILD_SAMPLES=OFF \
    -DAERON_BUILD_DOCUMENTATION=OFF

echo "--- Building..."
NPROC=$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
cmake --build . --parallel "${NPROC}" --target aeron aeron_driver aeronmd

# Collect outputs
mkdir -p "${AERON_OUT}/lib" "${AERON_OUT}/bin"

case "$(uname -s)" in
    Linux)
        cp -v "${AERON_BUILD}/lib/libaeron.so" "${AERON_OUT}/lib/" 2>/dev/null || true
        cp -v "${AERON_BUILD}/lib/libaeron_static.a" "${AERON_OUT}/lib/" 2>/dev/null || true
        cp -v "${AERON_BUILD}/lib/libaeron_driver.so" "${AERON_OUT}/lib/" 2>/dev/null || true
        ;;
    Darwin)
        cp -v "${AERON_BUILD}/lib/libaeron.dylib" "${AERON_OUT}/lib/" 2>/dev/null || true
        cp -v "${AERON_BUILD}/lib/libaeron_static.a" "${AERON_OUT}/lib/" 2>/dev/null || true
        cp -v "${AERON_BUILD}/lib/libaeron_driver.dylib" "${AERON_OUT}/lib/" 2>/dev/null || true
        ;;
esac

cp -v "${AERON_BUILD}/binaries/aeronmd" "${AERON_OUT}/bin/" 2>/dev/null || true

echo ""
echo "=== Build complete ==="
echo "  Library:      ${AERON_OUT}/lib/"
echo "  Media driver: ${AERON_OUT}/bin/aeronmd"
echo ""
echo "Usage:"
echo "  # Start media driver"
echo "  ${AERON_OUT}/bin/aeronmd"
echo ""
echo "  # Run aergo with library path"
echo "  go run ./cmd/aergo -lib ${AERON_OUT}/lib/libaeron.dylib"
echo ""
echo "  # Or set library search path"
echo "  export LD_LIBRARY_PATH=${AERON_OUT}/lib  # Linux"
echo "  export DYLD_LIBRARY_PATH=${AERON_OUT}/lib  # macOS"
