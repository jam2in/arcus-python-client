#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"

export COMPOSE_PROJECT_NAME=arcus-python-client-test
export ARCUS_TEST_RESULTS="$repository_root/build/integration/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$ARCUS_TEST_RESULTS"

compose() {
    docker compose -f "$repository_root/compose.yaml" "$@"
}

# Refuse to replace an in-progress test stack using the same project name.
if [ -n "$(compose ps --all --quiet)" ]; then
    printf '%s\n' 'The arcus-python-client-test project already has containers.' >&2
    printf '%s\n' 'Stop that test run before starting another one.' >&2
    exit 1
fi

cleanup() {
    test_status=$?
    trap - EXIT INT TERM
    compose logs --no-color > "$ARCUS_TEST_RESULTS/services.log" 2>&1 || true
    if ! compose down --volumes --remove-orphans --timeout 10; then
        test_status=1
    fi
    printf 'Integration results: %s\n' "$ARCUS_TEST_RESULTS"
    exit "$test_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

compose build tests
compose up --detach register
compose up --detach --wait --wait-timeout 120 cache1 cache2

# Save client output even if a test timeout aborts before JUnit XML is written.
test_status=0
compose run --rm --no-deps -T tests \
    python -m pytest tests/integration -v \
    --timeout=45 --timeout-method=thread \
    --junitxml=/results/integration.xml "$@" \
    > "$ARCUS_TEST_RESULTS/tests.log" 2>&1 || test_status=$?
cat "$ARCUS_TEST_RESULTS/tests.log"
exit "$test_status"
