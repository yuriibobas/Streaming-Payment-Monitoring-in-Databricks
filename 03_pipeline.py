# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Streaming-пайплайн
# MAGIC
# MAGIC Ноутбук **не запускається окремо**: `06_demo` підтягує його через `%run`.
# MAGIC
# MAGIC ```
# MAGIC landing/*.csv ──► BRONZE ──► SILVER ──► GOLD ──► ALERTS
# MAGIC  (нові файли)     сирі дані   чисті      схвалення   незвичні
# MAGIC                    як є       дані       по годинах  падіння
# MAGIC                               └─► QUARANTINE (биті рядки)
# MAGIC ```
# MAGIC
# MAGIC Два streaming-запити, обидва з `trigger(availableNow=True)` («обробити все нове з минулого разу і зупинитися»;
# MAGIC на serverless доступний саме цей режим):
# MAGIC
# MAGIC 1. **Ingest**: Auto Loader бере лише нові файли і в одному `foreachBatch` пише Bronze, Silver і Quarantine.
# MAGIC 2. **Gold**: читає Silver як потік, рахує схвалення у годинних вікнах за **часом події** з **watermark** і тримає **state** відкритих вікон.
# MAGIC
# MAGIC Після кожного оновлення `detect_alerts()` перевіряє нові вікна Gold.

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: правила якості даних
# MAGIC
# MAGIC Ту саму функцію `transform_silver` використовує і streaming, і batch (ноутбук 04): логіка однакова, різниться лише спосіб обробки.
# MAGIC
# MAGIC | `dq_reason` | Що не так |
# MAGIC |---|---|
# MAGIC | `invalid_event_time` | дата не існує, наприклад `2023-09-31` |
# MAGIC | `bad_count_orders` | кількість замовлень порожня або ≤ 0 |
# MAGIC | `success_with_error_code` | статус «успіх», але є код помилки |
# MAGIC | `unknown_status` | статус не з переліку success / fail / created / verify |

# COMMAND ----------

def transform_silver(df):
    """Розділити сирі рядки на (чисті з типами, биті з причиною). Повертає два DataFrame."""
    raw_cols = df.columns
    flagged = (df
               .withColumn("_ts", F.expr("try_to_timestamp(event_time, 'yyyy-MM-dd HH:mm:ss')"))
               .withColumn("_n", F.expr("try_cast(count_orders AS INT)"))
               .withColumn("dq_reason",
                           F.when(F.col("_ts").isNull(), "invalid_event_time")
                            .when(F.col("_n").isNull() | (F.col("_n") <= 0), "bad_count_orders")
                            .when((F.col("status") == "success") & F.col("error_code").isNotNull(), "success_with_error_code")
                            .when(~F.col("status").isin("success", "fail", "created", "verify"), "unknown_status")))

    # биті рядки: ОРИГІНАЛЬНІ значення + причина
    quarantine = flagged.where("dq_reason IS NOT NULL").select(*raw_cols, "dq_reason")

    top = CONFIG["top_countries"]
    country_grp = (F.when(F.col("card_country").isin(top), F.col("card_country"))
                    .when(F.col("card_country").isNull(), F.lit("UNKNOWN"))
                    .otherwise(F.lit("OTHER")))
    extra = [c for c in ["created_day", "_source_file", "_ingested_at"] if c in raw_cols]

    valid = (flagged.where("dq_reason IS NULL")
             .select(
                 F.col("_ts").alias("event_time"),
                 "channel_id", "order_type", "payment_source", "merchant_token_type",
                 F.expr("try_cast(is_secured AS BOOLEAN)").alias("is_secured"),
                 "bin_amount_usd", "status", "error_code", "error_message", "bank_issuer",
                 "card_brand", "card_type", "card_country", country_grp.alias("card_country_grp"), "ip_country",
                 (F.col("card_country").isNotNull() & (F.col("card_country") != F.col("ip_country"))).alias("country_mismatch"),
                 (F.col("retry_number") != "no_subscription").alias("is_subscription_charge"),
                 F.expr("try_cast(retry_number AS INT)").alias("retry_count"),
                 F.col("_n").alias("count_orders"),
                 F.expr("try_cast(count_emails AS INT)").alias("count_emails"),
                 *extra))
    return valid, quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC ## Запит 1 · Ingest: Bronze + Silver + Quarantine
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) пам'ятає (у checkpoint), які файли вже бачив. Якщо нових файлів немає, він прочитає **0 рядків**.
# MAGIC Bronze зберігає все як текст, без змін: це «чорна скринька» того, що прийшло.

# COMMAND ----------

def _ingest_batch(batch_df, batch_id):
    batch_df.write.mode("append").saveAsTable(T_BRONZE)
    valid, bad = transform_silver(batch_df)
    valid.write.mode("append").saveAsTable(T_SILVER)
    bad.write.mode("append").saveAsTable(T_QUAR)


def run_ingest():
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", f"{SCHEMA_LOC}/ingest")
              .option("header", True)
              .option("sep", ";")
              .load(LANDING)
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))
    q = (stream.writeStream
         .foreachBatch(_ingest_batch)
         .option("checkpointLocation", f"{CHECKPOINTS}/ingest")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    return stream_stats(q)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Запит 2 · Gold: схвалення в годинних вікнах
# MAGIC
# MAGIC ```python
# MAGIC silver_stream
# MAGIC   .withWatermark("event_time", "2 hours")                              # пізніше 2 год -> ігноруємо
# MAGIC   .groupBy(window("event_time", "1 hour"), "channel_id", "card_country_grp")
# MAGIC   .agg(sum(count_orders), sum(успішні), sum(відхилені))
# MAGIC ```
# MAGIC
# MAGIC * **Event time**: вікна будуються за часом платежу, а не за часом обробки.
# MAGIC * **State**: Spark тримає проміжні суми відкритих вікон між запусками й лише **доповнює** їх новими даними.
# MAGIC * **Watermark**: що старше за «найновіша подія мінус 2 години», вже запізно; такі рядки ігноруються, а пам'ять старих вікон звільняється.
# MAGIC * `outputMode("update")` + `foreachBatch` + `MERGE`: у Gold записуються лише вікна, які змінилися. Кожен рядок містить
# MAGIC   **повне** поточне значення вікна, тож повторний запуск того самого batch дає той самий результат.

# COMMAND ----------

def _ensure_gold_table():
    dim_cols = ", ".join(f"{d} STRING" for d in CONFIG["dims"])
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_GOLD} (
        window_start TIMESTAMP, window_end TIMESTAMP, {dim_cols},
        total_orders BIGINT, success_orders BIGINT, fail_orders BIGINT, approval_rate DOUBLE)""")


def _upsert_gold(batch_df, batch_id):
    keys = ["window_start"] + CONFIG["dims"]
    cond = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)          # <=> це «=», що коректно працює з NULL
    (DeltaTable.forName(batch_df.sparkSession, T_GOLD).alias("t")
        .merge(batch_df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def gold_aggregation(df):
    """Агрегація для Gold. Однакова для streaming (з watermark) і batch."""
    w = CONFIG["weight_col"]
    return (df.where(F.col("status").isin("success", "fail"))
              .groupBy(F.window(CONFIG["time_col"], CONFIG["window"]), *CONFIG["dims"])
              .agg(F.sum(w).alias("total_orders"),
                   F.sum(F.when(F.col("status") == "success", F.col(w)).otherwise(0)).alias("success_orders"),
                   F.sum(F.when(F.col("status") == "fail", F.col(w)).otherwise(0)).alias("fail_orders"))
              .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                      *CONFIG["dims"], "total_orders", "success_orders", "fail_orders",
                      F.expr("try_divide(success_orders, total_orders)").alias("approval_rate")))


def run_gold():
    if not table_exists(T_SILVER):
        print("Silver ще порожній: спершу має відпрацювати ingest.")
        return {"rows": 0, "batches": 0, "dropped_late": 0, "state_rows": 0, "state_mb": 0.0}
    _ensure_gold_table()
    silver_stream = spark.readStream.table(T_SILVER).withWatermark(CONFIG["time_col"], CONFIG["watermark"])
    q = (gold_aggregation(silver_stream).writeStream
         .outputMode("update")
         .foreachBatch(_upsert_gold)
         .option("checkpointLocation", f"{CHECKPOINTS}/gold")
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    return stream_stats(q)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Алерти: чи це вікно незвично погане?
# MAGIC
# MAGIC Кожне нове вікно Gold порівнюємо з **тим самим сегментом, тим самим днем тижня й тією самою годиною за попередні 4 тижні**
# MAGIC (тижневий ритм з EDA). Алерт, коли виконано **все**:
# MAGIC
# MAGIC 1. у вікні щонайменше `min_orders` замовлень, а в базовій лінії `min_baseline_orders` (дрібні сегменти не чіпаємо);
# MAGIC 2. схвалення нижче за норму щонайменше на `alert_drop_pp` п.п.;
# MAGIC 3. падіння статистично велике: `z = (rate − baseline) / sqrt(baseline · (1 − baseline) / n)` нижче за `−alert_z`
# MAGIC    (z показує, у скільки «звичайних випадкових коливань» вкладається падіння).
# MAGIC
# MAGIC `detected_at` = кінець вікна, тобто момент, коли година закрилася й алерт міг спрацювати.

# COMMAND ----------

def _ensure_alerts_table():
    dim_cols = ", ".join(f"{d} STRING" for d in CONFIG["dims"])
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_ALERTS} (
        alert_id STRING, window_start TIMESTAMP, window_end TIMESTAMP, {dim_cols},
        total_orders BIGINT, approval_pct DOUBLE, baseline_pct DOUBLE, drop_pp DOUBLE, z_score DOUBLE,
        lost_orders_est BIGINT, severity STRING, detected_at TIMESTAMP, processed_at TIMESTAMP, status STRING)""")


def detect_alerts(now, lookback_hours=48):
    """Знайти нові алерти серед вікон, що закрилися до `now`. Повертає pandas DataFrame з НОВИМИ алертами."""
    _ensure_alerts_table()
    if not table_exists(T_GOLD) or now is None:
        return pd.DataFrame()
    c = CONFIG
    dims = ", ".join(c["dims"])
    now_s = pd.Timestamp(now).strftime("%Y-%m-%d %H:%M:%S")
    days_back = 7 * c["baseline_weeks"] + 2
    candidates = spark.sql(f"""
        WITH g AS (
            SELECT *, dayofweek(window_start) AS dow, hour(window_start) AS hr
            FROM {T_GOLD}
            WHERE window_start >= TIMESTAMP'{now_s}' - INTERVAL {days_back} DAYS
              AND window_end   <= TIMESTAMP'{now_s}'
        ),
        b AS (
            SELECT *, SUM(total_orders)   OVER w AS baseline_orders,
                      SUM(success_orders) OVER w AS baseline_success
            FROM g
            WINDOW w AS (PARTITION BY {dims}, dow, hr ORDER BY window_start
                         ROWS BETWEEN {c['baseline_weeks']} PRECEDING AND 1 PRECEDING)
        ),
        s AS (
            SELECT *, try_divide(success_orders, total_orders)      AS rate,
                      try_divide(baseline_success, baseline_orders) AS baseline_rate
            FROM b
        ),
        z AS (
            SELECT *, (baseline_rate - rate) * 100 AS drop_pp,
                      try_divide(rate - baseline_rate, sqrt(baseline_rate * (1 - baseline_rate) / total_orders)) AS z_score
            FROM s
        )
        SELECT concat('ALR-', date_format(window_start, 'yyyyMMdd-HH'), '-', concat_ws('-', {dims})) AS alert_id,
               window_start, window_end, {dims}, total_orders,
               ROUND(rate * 100, 1)          AS approval_pct,
               ROUND(baseline_rate * 100, 1) AS baseline_pct,
               ROUND(drop_pp, 1)             AS drop_pp,
               ROUND(z_score, 1)             AS z_score,
               CAST(ROUND(total_orders * (baseline_rate - rate)) AS BIGINT) AS lost_orders_est,
               CASE WHEN drop_pp >= {c['p1_drop_pp']} THEN 'P1' ELSE 'P2' END AS severity,
               window_end                    AS detected_at,
               TIMESTAMP'{now_s}'            AS processed_at,
               'open'                        AS status
        FROM z
        WHERE window_start    >= TIMESTAMP'{now_s}' - INTERVAL {lookback_hours} HOURS
          AND total_orders    >= {c['min_orders']}
          AND baseline_orders >= {c['min_baseline_orders']}
          AND drop_pp         >= {c['alert_drop_pp']}
          AND z_score         <= -{c['alert_z']}
    """)
    (DeltaTable.forName(spark, T_ALERTS).alias("t")
        .merge(candidates.alias("s"), "t.alert_id = s.alert_id")
        .whenNotMatchedInsertAll()
        .execute())
    return (spark.table(T_ALERTS)
            .where(F.col("processed_at") == F.lit(now_s).cast("timestamp"))
            .orderBy("window_start").toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Як влаштований пайплайн: таблиці, колонки, розміри

# COMMAND ----------

LAYER_INFO = [
    ("Bronze", T_BRONZE, "#8c6d46", "Сирі дані як є, усі колонки текстом + звідки й коли прийшли"),
    ("Silver", T_SILVER, "#7f8c8d", "Чисті дані з правильними типами + допоміжні колонки"),
    ("Quarantine", T_QUAR, "#c0392b", "Биті рядки з оригінальними значеннями і причиною dq_reason"),
    ("Gold", T_GOLD, "#d4a017", "Замовлення, успішні, відхилені й схвалення за кожну годину й сегмент"),
    ("Alerts", T_ALERTS, "#e74c3c", "Вікна, де схвалення незвично впало"),
]


def _table_info(t):
    if not table_exists(t):
        return None
    d = spark.sql(f"DESCRIBE DETAIL {t}").first()
    return {"rows": spark.table(t).count(), "cols": len(spark.table(t).columns),
            "mb": (d["sizeInBytes"] or 0) / 1e6, "files": d["numFiles"] or 0}


def show_pipeline_overview():
    """Схема пайплайну з реальними розмірами кожного шару."""
    lf = landing_files()
    land_mb = sum(os.path.getsize(f"{LANDING}/{f}") for f in lf) / 1e6 if lf else 0

    def box(title, color, lines, desc):
        body = "".join(f"<div style='font-size:12px;color:#1a1a1a;margin-top:2px;'>{x}</div>" for x in lines)
        return (f"<div style='flex:1;min-width:140px;background:#ffffff;border:1px solid #e3e6ea;border-top:5px solid {color};"
                f"border-radius:8px;padding:10px 12px;'>"
                f"<div style='font-weight:700;font-size:15px;color:#1a1a1a;'>{title}</div>{body}"
                f"<div style='font-size:11px;color:#6b7785;margin-top:6px;'>{desc}</div></div>")

    arrow = "<div style='align-self:center;font-size:22px;color:#9aa5b1;'>➜</div>"
    parts = [box("Landing", "#5d6d7e", [f"файлів: <b>{n_fmt(len(lf))}</b>", f"розмір: <b>{land_mb:.1f} MB</b>"],
                 "погодинні CSV, які «прийшли»")]
    for title, t, color, desc in LAYER_INFO:
        i = _table_info(t)
        lines = ["таблиці ще немає"] if i is None else [
            f"рядків: <b>{n_fmt(i['rows'])}</b>", f"колонок: <b>{i['cols']}</b>",
            f"розмір: <b>{i['mb']:.1f} MB</b> · файлів: {i['files']}"]
        parts.append(box(title, color, lines, desc))
    html = parts[0] + arrow + parts[1] + arrow + parts[2] + arrow + parts[4] + arrow + parts[5]
    displayHTML(f"<div style='font-family:Arial,Helvetica,sans-serif;'>"
                f"<div style='display:flex;gap:8px;flex-wrap:wrap;'>{html}</div>"
                f"<div style='display:flex;gap:8px;margin-top:8px;'><div style='flex:1;'></div>"
                f"<div style='flex:1;max-width:360px;'>↳ {parts[3]}</div><div style='flex:2;'></div></div></div>")


def show_layer(name):
    """Колонки, типи й приклад рядків одного шару."""
    title, t, color, desc = next(x for x in LAYER_INFO if x[0].lower() == name.lower())
    if not table_exists(t):
        print(f"{title}: таблиці ще немає")
        return
    banner(f"<b>{title}</b> · <code>{t}</code><br>{desc}", color=color, bg="#f7f7f7")
    sch = pd.DataFrame([(f.name, f.dataType.simpleString()) for f in spark.table(t).schema.fields],
                       columns=["колонка", "тип"])
    display(sch)
    display(spark.table(t).limit(5))


def show_layers():
    for title, *_ in LAYER_INFO:
        show_layer(title)


def show_quarantine():
    if not table_exists(T_QUAR):
        print("Карантин порожній.")
        return
    display(spark.sql(f"""SELECT dq_reason AS `причина`, COUNT(*) AS `рядків`,
                                 SUM(try_cast(count_orders AS INT)) AS `замовлень`
                          FROM {T_QUAR} GROUP BY dq_reason"""))
    display(spark.table(T_QUAR).select("created_day", "event_time", "status", "count_orders", "dq_reason", "_source_file").limit(5))
