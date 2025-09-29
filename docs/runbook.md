# DataOps Runbook

Цель
- Надежно и воспроизводимо загружать исторические файлы (csv/xlsx) в TimescaleDB через этапы stage → validation → features → core.
- Давать ML и API быстрые представления (dbt-вью) и базовые признаки времени.

Структура каталогов
- data/raw/YYYY-MM-DD — сырье за конкретный день (рекомендуемый паттерн).
- artifacts/quality_reports — отчеты DQ (Great Expectations, HTML/JSON).
- artifacts/run_logs — логи запусков скриптов/контейнеров.
- infra — Dockerfile, docker-compose.yml, .env.
- etl — код конвейера (ingest, parse, enrich, load, publish).
- dbt — SQL-модели фич.
- expectations — Great Expectations suites/checkpoints.

Переменные окружения (infra/.env)
- DATABASE_URL — строка подключения к Postgres/Timescale.
- RAW_DIR — путь к каталогу сырых файлов внутри контейнера (по умолчанию /app/data/raw).
- STAGE_SCHEMA, QUALITY_SCHEMA, FEATURES_SCHEMA, CORE_SCHEMA — схемы БД.
- DAY_START, DAY_END — границы «дня» (час в 0–23).
- DELTA_T_MIN, DELTA_T_MAX — нормативы для delta_T.
- RUN_MODE — once или loop.
- REFRESH_OBJECTS — список объектов schema.name для обновления (CAGG/MV/VIEW).

Как запустить (Docker)
1) Подготовьте infra/.env (по образцу infra/.env.sample).
2) Сложите файлы в data/raw/YYYY-MM-DD (сегодняшняя дата для run_etl_today).
3) Разовый прогон за сегодня:
   - ./scripts/run_etl_today.sh
4) Историческая догрузка:
   - ./scripts/backfill_history.sh 2025-01-01 2025-01-07
5) Режим loop:
   - В .env установите RUN_MODE=loop и запустите контейнер dataops в compose.

Локальный запуск (без Docker)
- Установите зависимости из infra/requirements.txt, активируйте venv.
- Экспортируйте переменные окружения, например:
  - export DATABASE_URL=postgresql://user:pass@host:5432/db
  - export RAW_DIR="$(pwd)/data/raw/$(date +%F)"
  - python -m etl.run_etl
- Для backfill — меняйте RAW_DIR на нужный подкаталог и повторяйте запуск.

Мониторинг и проверка
- Логи ETL: artifacts/run_logs (скрипты) + stdout контейнера.
- Журнал загрузок: quality.quality_load_log (статус, счетчики, сообщения).
- Ошибки строк (если валидация расширена): quality.quality_row_errors.
- Быстрые sanity-чек запросы:
  - SELECT * FROM quality.quality_load_log ORDER BY started_at DESC LIMIT 20;
  - SELECT count(*) FROM stage.stage_raw_measurements WHERE load_id='{UUID}';
  - SELECT count(*) FROM stage.stage_parsed_measurements WHERE load_id='{UUID}' AND quality=0;
  - SELECT * FROM features.time_attributes ORDER BY ts DESC LIMIT 10;
  - SELECT * FROM features.ml_hourly_by_building ORDER BY ts DESC LIMIT 10;

Откат и повторная обработка
- Идемпотентность обеспечивается по checksum файла и upsert (time, meter_id, metric).
- Чтобы переобработать измененный файл:
  1) Обновите файл (checksum будет новым).
  2) Запустите ETL — файл пройдет заново.
- Чтобы принудительно «переиграть» тот же файл без изменения:
  1) Удалите запись о нем из quality.quality_load_log (status='success') и связанные строки из stage.stage_raw_measurements.
  2) Перезапустите ETL.

Распределение ролей
- DataOps (этот конвейер): файлы → stage → валидация/нормализация → core + фичи времени → публикация вью.
- Data Engineer: создает core-схему, справочники (buildings/itp/meters), hypertable measurements, конфигурирует CAGG/индексы.
- ML/Backend: читают из features.* и core.* согласованными SELECT’ами.

Типовые проблемы
- Нет маппинга external_code → id: строки не попадут в core.measurements (проверить справочники).
- Отсутствуют обязательные колонки в файлах: строки будут с reason и quality>0, попадут в quality_row_errors (при включении расширенного логирования).
- Конфликты схем/прав: убедиться в корректных схемах из .env, доступах БД, уникальном ключе (time,meter_id,metric) в core.measurements.

Ретеншн и хранение
- Сырые файлы: хранить в data/raw согласно политике (рекомендуется N месяцев).
- БД: ретеншн/компрессия — зона дата-инженера (политики Timescale).
- Логи: artifacts/run_logs — ротация через cron/systemd-journald или cleanup job.

Контакты и эскалация
- Ответственный за DataOps: [ФИО/контакт]
- Время реакции: в рабочее время или по SLA.