# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming-моніторинг платежів у Databricks · демо
# MAGIC
# MAGIC Єдиний ноутбук, який запускаємо на презентації. Увесь код живе в ноутбуках `00`–`05`, тут лише виклики функцій.
# MAGIC
# MAGIC | Частина | Що показуємо |
# MAGIC |---|---|
# MAGIC | 0 · Підготовка | запускається **до** презентації (≈15–20 хв) |
# MAGIC | 1 · Дані | що є в даних і що з цього випливає для моніторингу |
# MAGIC | 2 · Як це роблять batch | нічний звіт: проблему видно лише наступного ранку |
# MAGIC | 3 · Streaming-пайплайн | Bronze → Silver → Gold → Alerts: таблиці, колонки, розміри |
# MAGIC | 4 · Живий моніторинг | дані приходять щогодини, система сама піднімає тривогу |
# MAGIC | 5 · Batch проти streaming | обсяг роботи, швидкість, час виявлення, збережені гроші |
# MAGIC | 6 · Додатково | пізні дані, таблиця алертів |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_data_prep

# COMMAND ----------

# MAGIC %run ./02_eda

# COMMAND ----------

# MAGIC %run ./03_pipeline

# COMMAND ----------

# MAGIC %run ./04_batch

# COMMAND ----------

# MAGIC %run ./05_alerting

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Підготовка (запустити ДО презентації)
# MAGIC
# MAGIC 1. `prepare_data()`: нарізає CSV на погодинні файли й додає змодельований інцидент (≈ 3–5 хв, лише один раз).
# MAGIC 2. `eda_prepare()`: Delta-таблиця для графіків EDA.
# MAGIC 3. `run_batch_benchmark()`: кілька повних batch-перерахунків для порівняння (кілька хвилин, результат зберігається).
# MAGIC 4. `run_nightly_batch()`: нічний batch-звіт за день інциденту (результат зберігається).
# MAGIC 5. `reset_pipeline()` + `warm_up()`: прогнати через streaming усю історію до 14.09.2023, 08:00.
# MAGIC
# MAGIC Щоб повторити живу частину ще раз (наприклад, після репетиції), знову запустіть `reset_pipeline()` і `warm_up()`.

# COMMAND ----------

prepare_data()
eda_prepare()
run_batch_benchmark()
run_nightly_batch()
reset_pipeline()
warm_up()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Дані: що ми моніторимо
# MAGIC
# MAGIC Агреговані спроби оплати карткою за квітень–вересень 2023: статус, причина відмови, країна картки, канал, номер повторної спроби, діапазон суми.

# COMMAND ----------

eda_overview()

# COMMAND ----------

eda_monthly_trend()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Чому платежі не проходять

# COMMAND ----------

eda_retries()

# COMMAND ----------

eda_decline_reasons()

# COMMAND ----------

eda_amount()

# COMMAND ----------

eda_3ds()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Хто платить

# COMMAND ----------

eda_channels()

# COMMAND ----------

eda_payment_methods()

# COMMAND ----------

eda_countries()

# COMMAND ----------

eda_quick_facts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Тижневий ритм: головне для логіки алертів

# COMMAND ----------

eda_weekday()

# COMMAND ----------

eda_busy_days()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Як історичні дані стають «потоком»
# MAGIC
# MAGIC У CSV є лише дата, тому кожній події додано **синтетичний час доби**, а дані нарізано на **погодинні файли**.
# MAGIC Нова година даних = новий файл у папці `landing/`. Дати, денний і тижневий ритм справжні; розподіл подій усередині дня змодельований.

# COMMAND ----------

peek_staging()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Як це роблять batch: нічний звіт
# MAGIC
# MAGIC **Сценарій.** Четвер, 14.09.2023. О 10:00 у каналі 6 частина платежів карток із США починає відхилятися з кодом «Do not honor»
# MAGIC (так виглядає, наприклад, збій у еквайра). Збій триває до 16:00. *Інцидент змодельований, решта даних справжня.*
# MAGIC
# MAGIC Типова компанія без streaming рахує звіт **раз на добу**: о 06:00 batch-задача перечитує всю історію і показує вчорашній день.

# COMMAND ----------

show_nightly_report(run_nightly_batch())

# COMMAND ----------

# MAGIC %md
# MAGIC **А якщо запускати batch частіше, наприклад щогодини?** Кожен запуск усе одно перечитує всю історію, а вона росте.

# COMMAND ----------

plot_batch_cost()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Streaming-пайплайн
# MAGIC
# MAGIC ```
# MAGIC landing/ ──► BRONZE ──► SILVER ──► GOLD ──► ALERTS
# MAGIC               сирі       чисті     схвалення   незвичні
# MAGIC               дані       дані      по годинах  падіння
# MAGIC                          └─► QUARANTINE
# MAGIC ```
# MAGIC
# MAGIC Ключові рядки коду (повністю в `03_pipeline`):
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream.format("cloudFiles")          # Auto Loader: лише нові файли
# MAGIC   ...writeStream.foreachBatch(_ingest_batch)   # Bronze + Silver + Quarantine
# MAGIC   .trigger(availableNow=True)                  # обробити все нове і зупинитися
# MAGIC
# MAGIC spark.readStream.table(silver)
# MAGIC   .withWatermark("event_time", "2 hours")      # що пізніше, ігноруємо
# MAGIC   .groupBy(window("event_time", "1 hour"), "channel_id", "card_country_grp")
# MAGIC   .writeStream.outputMode("update")            # оновлюємо лише змінені вікна (MERGE у Gold)
# MAGIC ```
# MAGIC
# MAGIC Історія до 14.09.2023, 08:00 уже пройшла через пайплайн (частина 0). Ось як він виглядає зараз:

# COMMAND ----------

show_pipeline_overview()

# COMMAND ----------

show_layer("bronze")

# COMMAND ----------

show_layer("silver")

# COMMAND ----------

show_layer("gold")

# COMMAND ----------

# MAGIC %md
# MAGIC **Якість даних.** Рядки з датами, яких не існує (31 квітня, 31 червня, 31 вересня), не ламають потік: вони йдуть у карантин з оригінальними значеннями.

# COMMAND ----------

show_quarantine()

# COMMAND ----------

# MAGIC %md
# MAGIC **Це справді streaming?** Запустимо ті самі запити ще раз, без нових файлів.

# COMMAND ----------

rerun_without_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Живий моніторинг
# MAGIC
# MAGIC Зараз 14.09.2023, 08:00. Щогодини приходить нова година даних, пайплайн її обробляє, і система перевіряє кожен сегмент
# MAGIC (канал × країна картки) проти норми для цієї години цього дня тижня.

# COMMAND ----------

live_monitoring(hours=4)

# COMMAND ----------

plot_incident_day()

# COMMAND ----------

# MAGIC %md
# MAGIC Перемотаємо час до ранку наступного дня (саме тоді прийшов би нічний batch-звіт):

# COMMAND ----------

catch_up()

# COMMAND ----------

plot_incident_day()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Batch проти streaming
# MAGIC
# MAGIC ### Скільки роботи робить кожне оновлення

# COMMAND ----------

plot_batch_vs_streaming()

# COMMAND ----------

show_processing_diagram()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Коли ми дізнаємося про проблему і скільки це коштує

# COMMAND ----------

show_detection_timeline()

# COMMAND ----------

show_business_impact()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Додатково
# MAGIC
# MAGIC ### Пізні дані
# MAGIC Файл із квітневими платежами приходить лише у вересні. Watermark каже Gold, що ці вікна давно закриті.

# COMMAND ----------

demo_late_data()

# COMMAND ----------

show_alerts_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Висновки
# MAGIC
# MAGIC * **Streaming обробляє лише нове**: checkpoint пам'ятає оброблені файли, state зберігає відкриті вікна, watermark відсікає запізнілі дані.
# MAGIC   Обсяг роботи на одне оновлення не росте з історією, а в batch росте.
# MAGIC * **Проблему видно за годину, а не наступного ранку**, тож команда встигає зреагувати, поки інцидент ще триває.
# MAGIC * **Чесні обмеження**: час доби змодельований, дані «приходять» цілими годинами, інцидент штучний, а `availableNow`
# MAGIC   запускається вручну (на serverless немає постійно ввімкнених тригерів). На кластері той самий код працює безперервно, змінюється лише тригер.
# MAGIC * **Застосування до інших даних**: змінити `CONFIG` у `00_config`, підготовку файлів у `01_data_prep` і правила якості в `transform_silver`.
