#!/usr/bin/env bash
set -euo pipefail

# Запуск ETL для файлов за сегодня, лежащих в data/raw/YYYY-MM-DD
# По умолчанию использует docker compose. Если Docker недоступен, можно запустить локально:
#   DATABASE_URL=... RAW_DIR=... RUN_MODE=once python -m etl.run_etl

# Настройки
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="${REPO_ROOT}/infra"
RAW_ROOT="${REPO_ROOT}/data/raw"
TODAY="$(date +%F)"   # YYYY-MM-DD
RAW_DIR="${RAW_ROOT}/${TODAY}"
LOG_DIR="${REPO_ROOT}/artifacts/run_logs"
mkdir -p "${LOG_DIR}"

LOG_TS="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/run_etl_today-${LOG_TS}.log"

# Проверка наличия каталога с файлами
if [[ ! -d "${RAW_DIR}" ]]; then
  echo "Каталог с сырыми файлами на сегодня не найден: ${RAW_DIR}" | tee -a "${LOG_FILE}"
  echo "Создайте ${RAW_DIR} и положите туда *.csv/*.xlsx" | tee -a "${LOG_FILE}"
  exit 1
fi

# Выбор команды docker compose
if command -v docker &>/dev/null; then
  if docker compose version &>/dev/null; then
    DC="docker compose"
  elif command -v docker-compose &>/dev/null; then
    DC="docker-compose"
  else
    echo "Не найден docker compose" | tee -a "${LOG_FILE}"
    exit 1
  fi
else
  echo "Docker не установлен, попробуйте локальный запуск:
DATABASE_URL=... RAW_DIR='${RAW_DIR}' RUN_MODE=once python -m etl.run_etl" | tee -a "${LOG_FILE}"
  exit 1
fi

pushd "${INFRA_DIR}" >/dev/null

# Прогон ETL в режиме once, переопределяя RAW_DIR на подкаталог сегодняшней даты
echo "[INFO] Запуск ETL для RAW_DIR=${RAW_DIR}" | tee -a "${LOG_FILE}"
${DC} run --rm \
  -e RAW_DIR="/app/data/raw/${TODAY}" \
  -e RUN_MODE="once" \
  dataops 2>&1 | tee -a "${LOG_FILE}"

popd >/dev/null

echo "[INFO] Готово. Лог: ${LOG_FILE}"