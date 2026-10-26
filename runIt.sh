#!/usr/bin/env bash
# No `set -e` on purpose: `docker stop`/`docker rm` exit non-zero when the
# container is not running, which is normal on a first run. The secret check
# exits on its own.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${PREPROCESSOR_ENV_FILE:-$SCRIPT_DIR/.env}"

SECRET_VARS=(API_KEY)

# Secrets come from a gitignored .env beside this script (see README
# "Required secrets"). The file is a fallback: a value already exported in the
# environment survives sourcing, so `API_KEY=... ./runIt.sh` does what it says.
load_env_file() {
  [ -f "$ENV_FILE" ] || return 0

  local name saved=()
  for name in "${SECRET_VARS[@]}"; do
    saved+=("$name=${!name-}")
  done

  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a

  local pair value
  for pair in "${saved[@]}"; do
    name="${pair%%=*}"
    value="${pair#*=}"
    [ -n "$value" ] && printf -v "$name" '%s' "$value"
  done
}

load_env_file

# Checked FIRST, before stop/rm below: those take the running service down,
# so a missing or blank secret must abort while the old container still runs.
require_secret() {
  local name="$1" value="${!1-}"
  if [ -z "${value//[[:space:]]/}" ]; then
    echo "$(basename "$0"): $name is not set (or is blank)." >&2
    echo "  Set it in $ENV_FILE (see .env.example) or export it before running." >&2
    exit 1
  fi
}

for _secret in "${SECRET_VARS[@]}"; do
  require_secret "$_secret"
done

# `-e API_KEY` with no value makes docker read it from this environment, so the
# key never appears on a command line (ps, shell history).
export API_KEY

cd "$SCRIPT_DIR" || exit 1

docker stop sermonpreprocessor
docker rm sermonpreprocessor

docker build -t sermonpreprocessor .

docker run -dit --restart unless-stopped --name sermonpreprocessor -p 5060:5060 \
  -v ./data:/data \
  -e API_KEY \
  sermonpreprocessor:latest
