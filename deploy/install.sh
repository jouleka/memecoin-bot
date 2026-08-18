#!/usr/bin/env bash
# Generic one-time install for a checkout at /opt/memecoin-bot.
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "run this reviewed script as root" >&2
  exit 1
fi

command -v uv >/dev/null 2>&1 || {
  echo "uv is required; install it from https://docs.astral.sh/uv/ first" >&2
  exit 1
}

install_root=/opt/memecoin-bot
service_user=memebot

[[ -d ${install_root}/.git ]] || {
  echo "expected a Git checkout at ${install_root}" >&2
  exit 1
}

id -u "${service_user}" >/dev/null 2>&1 \
  || useradd --system --home "${install_root}" --shell /usr/sbin/nologin "${service_user}"

export UV_PYTHON_INSTALL_DIR="${install_root}/.uv-python"
cd "${install_root}"
uv python install 3.13
uv sync --locked --no-dev

if [[ ! -e .env ]]; then
  install -o root -g "${service_user}" -m 0640 /dev/null .env
else
  chown root:"${service_user}" .env
  chmod 0640 .env
fi
install -d -o "${service_user}" -g "${service_user}" -m 0750 data

install -o root -g root -m 0644 deploy/memecoin-bot.service \
  /etc/systemd/system/memecoin-bot.service
systemctl daemon-reload
systemctl enable --now memecoin-bot
systemctl status memecoin-bot --no-pager
