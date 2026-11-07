#!/usr/bin/env bash
# Verify a checkout against disposable local infrastructure.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
repo=$PWD
work=$(mktemp -d "${TMPDIR:-/tmp}/rwl-release.XXXXXXXX")
project="rwl-release-${work##*.}"
project=${project,,}
provider_pid=
compose_started=false
compose=(docker compose --env-file "$repo/.env.example" -f "$repo/compose.yaml" -p "$project")

cleanup() {
    local status=$?
    trap - EXIT
    if [[ -n $provider_pid ]]; then
        kill "$provider_pid" 2>/dev/null || true
        wait "$provider_pid" 2>/dev/null || true
    fi
    if [[ $compose_started == true ]]; then
        if (( status != 0 )); then
            "${compose[@]}" logs --no-color > "$work/compose.log" 2>&1 || true
        fi
        if ! "${compose[@]}" down --volumes; then
            printf 'Could not remove Compose project %s.\n' "$project" >&2
            status=1
        fi
    fi
    rm -rf -- "$work/venv"
    printf 'Verification logs and reports: %s\n' "$work"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

unset PYTHONPATH PYTHONHOME PYTHONOPTIMIZE PYTEST_ADDOPTS
export PGHOST=127.0.0.1 PGDATABASE=retry_window_lab PGUSER=rwl_dev
export PGPORT="${RWL_VERIFY_PG_PORT:-55433}"
export RWL_REDIS_PORT="${RWL_VERIFY_REDIS_PORT:-56380}"
export REDIS_URL="redis://127.0.0.1:$RWL_REDIS_PORT/0"
export RWL_APP_USER=rwl_app RWL_PROVIDER_USER=rwl_provider_service
PGPASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
RWL_APP_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
RWL_PROVIDER_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
export PGPASSWORD RWL_APP_PASSWORD RWL_PROVIDER_PASSWORD
provider_port=${RWL_VERIFY_PROVIDER_PORT:-18001}
export RWL_PROVIDER_URL="http://127.0.0.1:$provider_port"

python3 - "$PGPORT" "$RWL_REDIS_PORT" "$provider_port" <<'PY'
from contextlib import ExitStack
import socket
import sys

if sys.version_info < (3, 12):
    raise SystemExit("Use Python 3.12 or newer.")
with ExitStack() as stack:
    for value in sys.argv[1:]:
        if not value.isdecimal() or not 1024 <= int(value) <= 65535:
            raise SystemExit(f"Invalid verification port {value!r}. Use 1024–65535.")
        listener = stack.enter_context(socket.socket())
        try:
            listener.bind(("127.0.0.1", int(value)))
        except OSError:
            raise SystemExit(f"Port {value} is unavailable. Choose another verification port.") from None
        listener.listen()
PY

"${compose[@]}" config --quiet
python3 -m venv "$work/venv"
python="$work/venv/bin/python"
"$python" -m pip install -r requirements.lock
"$python" -m pip install --no-deps -e .
"$python" -m pip check

compose_started=true
"${compose[@]}" up -d --wait --wait-timeout 120
"$python" -m retry_window_lab.provision
"$python" -m pytest -q | tee "$work/pytest.log"

"$python" -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 \
    --port "$provider_port" > "$work/provider.log" 2>&1 &
provider_pid=$!
"$python" - "$provider_pid" "$RWL_PROVIDER_URL" <<'PY'
import json
import os
import sys
from time import monotonic, sleep
from urllib.request import urlopen

pid, base_url = sys.argv[1:]
deadline = monotonic() + 15
while monotonic() < deadline:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        break
    try:
        with urlopen(base_url + "/health", timeout=1) as response:
            if json.load(response) == {"status": "ok"}:
                break
    except OSError:
        sleep(0.1)
else:
    raise SystemExit("Provider simulator readiness timed out.")
try:
    os.kill(int(pid), 0)
except ProcessLookupError:
    raise SystemExit("Provider simulator exited. Check provider.log.") from None
PY

"$python" -m retry_window_lab.recovery_demo | tee "$work/recovery.log"
"$python" -m retry_window_lab.capacity_demo | tee "$work/capacity.log"
"$python" -m retry_window_lab.comparison --seed 42 --output "$work/comparison"
"$python" -m retry_window_lab.benchmark --slots 1 --repeats 1 --warmup 4 \
    --tasks 12 --output "$work/benchmark"

"$python" - "$work/comparison/comparison.json" <<'PY'
import json
from pathlib import Path
import sys

report = json.loads(Path(sys.argv[1]).read_text())
expected = {"fresh-key": (2, 5), "same-key": (3, 4), "evidence": (6, 0)}
actual = {
    name: (row["confirmed_single_effect_completions"], row["duplicate_effects"])
    for name, row in report["summary"].items()
}
if len(report["trials"]) != 24 or actual != expected:
    raise SystemExit("Policy comparison differed from the verified baseline.")
PY

git diff --check
printf 'Release verification passed.\n'
