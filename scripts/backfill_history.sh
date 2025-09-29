#!/usr/bin/env bash
set -euo pipefail

# Историческая догрузка: итерация по датам и запуск ETL для data/raw/YYYY-MM-DD
# Использование:
#   scripts/backfill_history.sh 2025-01-01 2025-01-07
# Примечание: если GNU date недоступен (macOS), скрипт fallback на Python для генерации дат.

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE  (формат YYYY-MM-DD)"
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="${REPO_ROOT}/infra"
RAW_ROOT="${REPO_ROOT}/data/raw"
LOG_DIR="${REPO_ROOT}/artifacts/run_logs"
mkdir -p "${LOG_DIR}"

# Выбор команды docker compose
if command -v docker &>/dev/null; then
  if docker compose version &>/dev/null; then
    DC="docker compose"
  elif command -v docker-compose &>/dev/null; then
    DC="docker-compose"
  else
    echo "Не найден docker compose"; exit 1
  fi
else
  echo "Docker не установлен. Для локального режима запускайте для каждого дня:
DATABASE_URL=... RAW_DIR=... RUN_MODE=once python -m etl.run_etl"
  exit 1
fi

# Генератор дат в диапазоне (перекрываем различия GNU/BSD date)
gen_dates() {
  python3 - "$START_DATE" "$END_DATE" << 'PYCODE'
import sys, datetime
s = datetime.date.fromisoformat(sys.argv[1])
e = datetime.date.fromisoformat(sys.argv[2])
if s > e:
    sys.exit("START_DATE > END_DATE")
d = s
while d <= e:
    print(d.isoformat())
    d += datetime.timedelta(days=1)
PYCODE
}

pushd "${INFRA_DIR}" >/dev/null

for day in $(gen_dates); do
  RAW_DIR_DAY="${RAW_ROOT}/${day}"
  LOG_FILE="${LOG_DIR}/backfill_${day}.log"
  if [[ ! -d "${RAW_DIR_DAY}" ]]; then
    echo "[WARN] Пропуск: нет каталога ${RAW_DIR_DAY}" | tee -a "${LOG_FILE}"
    continue
  fi
  echo "[INFO] Backfill ${day} (RAW_DIR=${RAW_DIR_DAY})" | tee -a "${LOG_FILE}"
  ${DC} run --rm \
    -e RAW_DIR="/app/data/raw/${day}" \
    -e RUN_MODE="once" \
    dataops 2>&1 | tee -a "${LOG_FILE}"
done

popd >/dev/null

echo "[INFO] Backfill завершен. Логи: ${LOG_DIR}/backfill_*.log"