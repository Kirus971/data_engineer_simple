## SCD Type 2 в этом шаблоне

Таблица: `hms_cat.silver.dim_customer`

### Колонки

- **business_key**: стабильный ключ (здесь `customer_id` как строка)
- **hashdiff**: хэш атрибутов, чтобы дешево определять изменения
- **effective_from / effective_to**: период действия версии
- **is_current**: текущая активная версия

### Логика инкремента

- Источник инкремента: `hms_cat.bronze.customer_cdc` (Kafka->Iceberg)
- Водяной знак (watermark) хранится в Iceberg таблице: `hms_cat.meta.watermarks`
  - `pipeline = dim_customer_scd2`
  - `updated_at_gte` используется как нижняя граница для следующего запуска

### MERGE стратегия

В `spark/jobs/scd2_merge_customers_silver.py` используется 2 шага:

1) `MERGE` закрывает текущую запись, если пришли измененные атрибуты.
2) Затем `INSERT` добавляет новую “current” версию для закрытых ключей.

Это сделано потому что не во всех комбинациях версий Iceberg/Spark удобно выразить
“matched -> update + insert” в одном `MERGE` без сложных конструкций.

