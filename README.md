ETL Parsing Service — Подробная техническая документация

1. Краткое резюме
Документ описывает архитектуру и текущее состояние сервиса парсинга, реализованного в репозитории.
Сервис превращает сырые Excel/CSV-файлы с показаниями приборов учёта в нормализованные записи,
которые загружаются в PostgreSQL и агрегируются для последующего построения витрин и ML-фич.
Анализ основан на файлах: etl/*, dbt/models/*, etl/sql/init_core.sql, scripts/*, infra/*, expectations/*.

2. Архитектура и поток данных
Кратко: data/raw -> stage -> core -> features (dbt)
Ключевые скрипты:
 - etl/run_etl.py — главный запуск; шаги: ingest, parse, enrich, load, publish
 - etl/flows/ingest_from_files.py — регистрация файлов, генерация load_id
 - etl/flows/parse_and_normalize.py — парсинг и нормализация строк
 - etl/flows/enrich_features.py — обогащение временными признаками
 - etl/flows/load_to_core.py — запись справочников и core.measurements
 - etl/flows/publish_views.py — создание views/materialized views в core

3. Подробное описание шагов пайплайна
3.1 Ingest
 - Сканирование RAW_DIR; формирование записей в stage.stage_raw_files с load_id и метаданными файла.
 - Поддерживаемые форматы: *.xlsx, *.xls, *.csv. Эвристика определения диапазона дат (detected_from, detected_to).

3.2 Parse
 - Чтение первых листов Excel/CSV через pandas (openpyxl для xlsx).
 - Автоопределение колонок: ts, building, itp, meter, metric, value, unit по вхождению ключевых слов.
 - Нормализация: normalize_entity_code, normalize_meter_code, normalize_metric; очистка числовых значений (_safe_num).
 - Результат: запись в stage.stage_parsed_measurements (load_id, row_num, ts, building_code, itp_code, meter_code, metric, value, unit).

3.3 Enrich
 - На базе stage.stage_parsed_measurements формируется stage.stage_parsed_measurements_enriched.
 - Добавляются признаки: ts_hour (усечённый до часа), dow (день недели), is_weekend (булево).

3.4 Load
 - Создаются/проверяются справочники: core.buildings, core.itp, core.meters.
 - Вставка фактов в core.measurements: (measurement_id, meter_id, ts, value, inserted_at).
 - Идемпотентность: ON CONFLICT (meter_id, ts) DO NOTHING.

3.5 Publish
 - Формируются core.measurements_flat (view), core.daily_balance и core.hourly_balance (materialized).
 - Эти объекты используются DBT-моделями для формирования features.

4. Описание схем и таблиц (ключевые DDL)
 - stage.stage_raw_files: load_id, file_path, file_name, detected_from, detected_to, rows, inserted_at
 - stage.stage_parsed_measurements: load_id, source_file, row_num, ts, building_code, itp_code, meter_code, metric, value, unit
 - stage.stage_parsed_measurements_enriched: load_id, row_num, ts_hour, dow, is_weekend, inserted_at
 - core.buildings: building_id, external_code, district_id
 - core.itp: itp_id, building_id, external_code
 - core.meters: meter_id, itp_id, external_code, metric, unit
 - core.measurements: measurement_id, meter_id, ts, value, inserted_at (unique constraint on meter_id+ts)

5. DBT-модели и витрины для ML
 - dbt/models/features/ml_daily_by_building.sql
 - dbt/models/features/ml_hourly_by_building.sql
 - dbt/models/features/ml_hourly_by_district.sql
 - Модели используют core.daily_balance и core.hourly_balance и добавляют временные атрибуты.

6. Проверки качества и expectations
 - В репозитории есть expectations/suites и checkpoints (stage_parsed_measurements.json, stage_parsed_checkpoint.yml).
 - В коде присутствуют базовые validation utils (etl/utils/validation.py) — is_reasonable_value, parse_timestamp и т.д.

7. Инфраструктура и запуск
 - Конфигурация через .env (infra/.env.sample). Ключевые переменные: DATABASE_URL, RAW_DIR, STAGE_SCHEMA, CORE_SCHEMA и т.д.
 - Скрипты: scripts/run_etl_today.sh, scripts/backfill_history.sh, scripts/cron_samples.txt
 - Docker: infra/Dockerfile и infra/docker-compose.yml. Контейнер dataops запускает ETL в контейнере.
 - Пример запуска локально: DATABASE_URL=postgresql://... RAW_DIR=/path/to/data/raw python -m etl.run_etl --steps ingest,parse,enrich,load,publish

8. Что реализовано (MVP)
 - Парсинг посуточных ведомостей (Excel) и нормализация строк в stage.
 - Инвентаризация загрузок (stage.stage_raw_files) и управление load_id.
 - Обогащение временными признаками и подготовка агрегатов (daily/hourly).
 - Идемпотентная загрузка фактов в core.measurements.
 - Подготовка dbt-моделей для формирования ML-ready витрин.

9. Что из ТЗ пока НЕ реализовано (планы)
 - Детектирование и классификация аномалий (>10% рассогласования ХВС ИТП vs ОДПУ ГВС) и логика алертов.
 - ML-модели прогнозирования технологических ситуаций и оценка вероятности инцидента.
 - Визуализация: интерактивные графики, дашборды и экспорт отчетов.
 - Рекомендательная система для диспетчеров с человекочитаемыми рекомендациями.
 - Интерфейс (UI/UX) с возможностью пометки/обратной связи и переобучения моделей.
 - Потоковая интеграция (УСПД/АСУПР) — сейчас только файловый импорт.
 - Полноценная автоматизация качества (Great Expectations в проде) и мониторинг.

10. Трудности и риски, обнаруженные в коде
 - Эвристический парсинг Excel: может ломаться на нестандартных файлах (разные листы, формат колонок, merged cells).
 - Timezones: parse_timestamp обрабатывает ISO и dateutil, но входные данные могут не содержать TZ; нужно явное указание DEFAULT_TZ и сохранение tz-aware timestamps.
 - Несогласованность канонических имён метрик: etl/utils/units.normalize_metric_unit использует иные каноники (flow_supply, consumption_period) чем parse logic (SUPPLY/CONSUMPTION/T1 и т.д.).
 - Идемпотентность реализована через ON CONFLICT DO NOTHING, но для обновлений/коррекции данных механизм upsert/soft-delete может потребоваться в будущем.
 - Масштабируемость: текущая пакетная модель не оптимальна для high-throughput streaming (нужны bulk inserts и очередь сообщений).
 - Mapping external IDs: интеграция с ФИАС/УНОМ/внешними реестрами не реализована; это ограничивает точность сопоставления по адресам/ИД.

11. Рекомендации и дальнейшие шаги
1) Унифицировать канонику метрик и единиц: согласовать parse -> units -> dbt.
2) Добавить source-specific mappings: конфиг для каждого типа входного файла, чтобы парсинг был предсказуем.
3) Сделать timezone-aware timestamps и сохранять TZ в БД.
4) Внедрить Great Expectations как шаг после parse и хранить отчеты в artifacts/quality_reports.
5) Разработать ML-pipeline: feature store, обучение/валидация моделей, CI/CD для моделей.
6) Планировать потоковую архитектуру (API/Kafka) для интеграции с УСПД/АСУПР.
7) Добавить мониторинг (Prometheus/Grafana) и логирование ошибок в central log.

12. Приложение: ключевые файлы и где смотреть
 - etl/run_etl.py
 - etl/flows/ingest_from_files.py
 - etl/flows/parse_and_normalize.py
 - etl/flows/enrich_features.py
 - etl/flows/load_to_core.py
 - etl/flows/publish_views.py
 - etl/sql/init_core.sql
 - etl/utils/config.py, db.py, io.py, logger.py, schema.py, units.py, validation.py
 - dbt/models/features/*.sql
 - expectations/suites/*.json, expectations/checkpoints/*.yml
 - infra/Dokerfile, infra/docker-compose.yml, infra/.env.sample
 - scripts/run_etl_today.sh, scripts/backfill_history.sh, scripts/cron_samples.txt
 - docs/runbook.md, docs/data_contract.md