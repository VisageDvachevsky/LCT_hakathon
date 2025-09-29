# Data Contract: Исторические файлы и загрузка в БД

Назначение
- Зафиксировать формат входных исторических данных, правила нормализации и контракт записи в core.measurements.

Формат входных файлов
- Поддерживаемые типы: CSV, XLSX.
- Кодировка: UTF-8 (CSV).
- Разделитель (CSV): запятая или точка с запятой (автоопределение pandas); десятичный разделитель — точка/запятая (автоконверсия).
- Заголовок: первая строка — имена колонок.

Обязательные поля (с алиасами)
- timestamp (ts, datetime, date_time)
- building_code (building, building_id)
- itp_code (itp, itp_id, heat_unit)
- meter_code (meter, meter_id, sensor_code)
- metric (metric, parameter, measure)
- value (value, val, reading)
- unit (unit, units, uom) — опционально, если известно по справочнику; при отсутствии нормализуем по дефолту.

Дополнительные поля
- tz (timezone) — если задан, используется; иначе — DEFAULT_TZ из окружения.
- extra — любые дополнительные метаданные (JSON/текст), сохраняются в stage_raw.

Справочники (core, зона дата-инженера)
- public.buildings(id, external_code, name, district_id, address, ...)
- public.itp(id, building_id, external_code, ...)
- public.meters(id, itp_id, external_code, metric, unit, ...)
- public.measurements(time timestamptz, building_id int, itp_id int, meter_id int, metric text, value double precision, unit text, quality smallint, source text, inserted_at timestamptz)
  - Требования:
    - Уникальный ключ: (time, meter_id, metric)
    - Индексы по time для time_bucket и CAGG
    - Дополнительно: индексы по (time, building_id), (time, itp_id) — по согласованию

Поддерживаемые метрики и единицы (нормализация)
- consumption_period — м3 (за интервал)
- consumption_cumulative — м3 (счетчик)
- flow_supply, flow_return — м3/ч
- T1, T2 — °C
- pump_runtime_hours — час
- Прочие единицы конвертируются при необходимости (например, л/с → м3/ч; degC → °C). Неизвестные единицы — помечаются, строки уходят в валидацию.

Валидации (stage → parsed)
- timestamp: валидный ISO8601; без TZ — локализуется по DEFAULT_TZ.
- коды: building_code/itp_code/meter_code/metric — не пустые.
- value: число; физически разумные диапазоны:
  - T1,T2 ∈ [-20, 150]
  - flow/consumption/pump_runtime_hours ≥ 0
- delta_T (производная для DQ): [0, 80] — мягкая проверка.
- Качество:
  - quality=0 — валидная строка.
  - quality=2 — ошибка валидации (reason указывается).

Маппинг в core (load_to_core)
- Join по external_code: (building_code, itp_code, meter_code, metric) → (building_id, itp_id, meter_id).
- Источник: source='file'; quality — 0 для валидных строк.
- Идемпотентность: ON CONFLICT (time, meter_id, metric) DO UPDATE value/unit/quality/source.

Гарантии и SLA
- Идемпотентная обработка: повторные запуски безопасны.
- Отслеживание загрузок: quality.quality_load_log (status ok/fail, counters).
- Ошибочные строки (опционально расширяем): quality.quality_row_errors с reason и payload.

Ожидаемая организация файлов
- Каталог data/raw/YYYY-MM-DD, где YYYY-MM-DD — дата измерений.
- Допустимы подкаталоги по домам/районам — допускается, все *.csv/*.xlsx будут прочитаны рекурсивно (по текущей реализации — плоский паттерн в каталоге; при необходимости делаем рекурсивный режим).

Совместимость и версии
- Postgres 16, TimescaleDB 2.15+.
- Python 3.11, pandas 2.2+.
- Изменения контракта — через MR и версионирование схем/моделей dbt.

Контакты
- Владелец контракта (DataOps): [ФИО/контакт]