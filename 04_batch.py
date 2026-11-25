# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Batch-підхід і порівняння зі streaming
# MAGIC
# MAGIC Ноутбук **не запускається окремо**: `06_demo` підтягує його через `%run`.
# MAGIC
# MAGIC **Batch** = щоразу брати **всі** сирі файли з початку історії, чистити їх і рахувати все заново.
# MAGIC Логіка очищення й агрегації та сама, що й у streaming (`transform_silver`, `gold_aggregation` з ноутбука 03),
# MAGIC тож порівнюємо лише **спосіб обробки**.
# MAGIC
# MAGIC Тут є два сценарії:
# MAGIC 1. **Нічний звіт** (типово для компаній без streaming): раз на добу о 06:00 перераховуємо все й дивимося на вчорашній день.
# MAGIC 2. **Що, якби запускати batch щогодини?** Вимірюємо, скільки даних і часу займає кожен повний перерахунок, коли історія росте.

# COMMAND ----------

import matplotlib.pyplot as plt

MODE_LABELS = {"batch_full": "batch: повний перерахунок", "batch_nightly": "batch: нічний звіт", "stream_tick": "streaming: оновлення"}


def _raw_paths_until(until):
    until = pd.Timestamp(until)
    return [f"{STAGING}/{f}" for f in staged_files() if file_hour(f) < until]


def _read_raw(paths):
    return (spark.read.option("header", True).option("sep", ";").csv(paths)
            .withColumn("_source_file", F.col("_metadata.file_path")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Сценарій 1 · Нічний batch-звіт

# COMMAND ----------

def run_nightly_batch(day=None, use_cache=True):
    """Нічний звіт: о 06:00 перераховуємо всю історію і шукаємо сегменти, де вчора схвалення було незвично низьким."""
    day = pd.Timestamp(day or SCENARIO["day"])
    report_at = day + pd.Timedelta(days=1, hours=SCENARIO["batch_report_hour"])
    cache = f"{CACHE_DIR}/nightly_{day:%Y-%m-%d}.json"
    if use_cache and os.path.exists(cache):
        res = json.load(open(cache))
        res["report"] = pd.DataFrame(res["report"])
        print(f"(результат нічного запуску взято з кешу: {res['seconds']:.0f} с, {n_fmt(res['files'])} файлів)")
        return res

    c, b = CONFIG, BATCH_CONFIG
    dims = ", ".join(c["dims"])
    t0 = time.time()
    paths = _raw_paths_until(day + pd.Timedelta(days=1))
    valid, _ = transform_silver(_read_raw(paths))
    daily = (valid.where(F.col("status").isin("success", "fail"))
             .groupBy(F.to_date("event_time").alias("day"), *c["dims"])
             .agg(F.sum("count_orders").alias("total_orders"),
                  F.sum(F.when(F.col("status") == "success", F.col("count_orders")).otherwise(0)).alias("success_orders")))
    daily.createOrReplaceTempView("_batch_daily")
    report = spark.sql(f"""
        WITH b AS (
            SELECT *, SUM(total_orders)   OVER w AS baseline_orders,
                      SUM(success_orders) OVER w AS baseline_success
            FROM _batch_daily
            WINDOW w AS (PARTITION BY {dims}, dayofweek(day) ORDER BY day
                         ROWS BETWEEN {c['baseline_weeks']} PRECEDING AND 1 PRECEDING)
        ),
        s AS (
            SELECT *, success_orders / total_orders AS rate, try_divide(baseline_success, baseline_orders) AS baseline_rate FROM b
        ),
        z AS (
            SELECT *, (baseline_rate - rate) * 100 AS drop_pp,
                      try_divide(rate - baseline_rate, sqrt(baseline_rate * (1 - baseline_rate) / total_orders)) AS z_score
            FROM s
        )
        SELECT {dims}, total_orders, ROUND(rate * 100, 1) AS approval_pct, ROUND(baseline_rate * 100, 1) AS baseline_pct,
               ROUND(drop_pp, 1) AS drop_pp, ROUND(z_score, 1) AS z_score
        FROM z
        WHERE day = DATE'{day:%Y-%m-%d}'
          AND total_orders >= {b['min_orders']} AND baseline_orders >= {b['min_baseline_orders']}
          AND drop_pp >= {b['alert_drop_pp']} AND z_score <= -{b['alert_z']}
        ORDER BY drop_pp DESC
    """).toPandas()
    seconds = time.time() - t0
    rows = _read_raw(paths).count()                     # окремо, щоб не впливало на заміряний час
    record_bench("batch_nightly", report_at, len(paths), rows, seconds)
    res = {"day": f"{day:%Y-%m-%d}", "report_at": f"{report_at:%Y-%m-%d %H:%M}", "seconds": seconds,
           "files": len(paths), "rows": int(rows), "report": json.loads(report.to_json(orient="records"))}
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache, "w") as f:
        json.dump(res, f)
    res["report"] = report
    return res


def show_nightly_report(res):
    """Як нічний звіт виглядає для команди."""
    rep = res["report"]
    report_at = pd.Timestamp(res["report_at"])
    incident_start = pd.Timestamp(SCENARIO["day"]) + pd.Timedelta(hours=SCENARIO["from_hour"])
    delay_h = (report_at - incident_start).total_seconds() / 3600
    rows_html = "".join(
        f"<tr><td style='padding:4px 10px;'>канал {r['channel_id']} · {r['card_country_grp']}</td>"
        f"<td style='padding:4px 10px;text-align:right;'>{n_fmt(r['total_orders'])}</td>"
        f"<td style='padding:4px 10px;text-align:right;color:#c0392b;font-weight:700;'>{r['approval_pct']}%</td>"
        f"<td style='padding:4px 10px;text-align:right;'>{r['baseline_pct']}%</td>"
        f"<td style='padding:4px 10px;text-align:right;'>−{r['drop_pp']} п.п.</td></tr>"
        for _, r in rep.iterrows()) or "<tr><td colspan='5' style='padding:6px 10px;'>Відхилень не знайдено</td></tr>"
    displayHTML(f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:780px;border:1px solid #d6dbe1;border-radius:8px;background:#ffffff;color:#1a1a1a;">
      <div style="background:#f3f5f8;padding:10px 16px;border-bottom:1px solid #d6dbe1;border-radius:8px 8px 0 0;">
        <span style="font-size:16px;font-weight:700;">📄 Щоденний звіт про платежі за {pd.Timestamp(res['day']):%d.%m.%Y}</span><br>
        <span style="font-size:12px;color:#5f6b7a;">Batch-задача · сформовано {report_at:%d.%m.%Y о %H:%M} ·
        перераховано {n_fmt(res['files'])} файлів, {n_fmt(res['rows'])} рядків за {res['seconds']:.0f} с</span>
      </div>
      <div style="padding:10px 16px;">
        <div style="font-size:13px;margin-bottom:6px;">Сегменти з незвично низьким схваленням за добу:</div>
        <table style="border-collapse:collapse;font-size:13px;">
          <tr style="background:#f3f5f8;"><th style="padding:4px 10px;text-align:left;">сегмент</th><th style="padding:4px 10px;">замовлень</th>
          <th style="padding:4px 10px;">схвалення</th><th style="padding:4px 10px;">норма</th><th style="padding:4px 10px;">падіння</th></tr>
          {rows_html}
        </table>
        <div style="margin-top:10px;padding:8px 12px;background:#fff4e5;border-left:4px solid #f39c12;font-size:13px;">
          ⏱️ Проблема почалася <b>{incident_start:%d.%m о %H:%M}</b>, а команда дізнається про неї лише
          <b>{report_at:%d.%m о %H:%M}</b>: через <b>{delay_h:.0f} год</b>. Інцидент за цей час уже скінчився сам, і всі втрати вже сталися.
        </div>
      </div>
    </div>""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Сценарій 2 · А якщо запускати batch щогодини?

# COMMAND ----------

def run_batch_benchmark(points=None, force=False):
    """Кілька повних перерахунків на різних етапах історії. Повільно (хвилини), тому результат зберігається в benchmark_runs."""
    if points is None:
        points = ["2023-05-01", "2023-06-15", "2023-08-01", f"{SCENARIO['day']} {SCENARIO['live_from_hour']:02d}:00"]
    if table_exists(T_BENCH) and not force:
        if spark.table(T_BENCH).where("mode = 'batch_full'").count() >= len(points):
            print("✅ Заміри batch уже є (force=True, щоб переміряти).")
            return
    if force and table_exists(T_BENCH):
        spark.sql(f"DELETE FROM {T_BENCH} WHERE mode = 'batch_full'")
    print("Вимірюємо повний batch-перерахунок (це кілька хвилин) ...")
    for as_of in points:
        t0 = time.time()
        paths = _raw_paths_until(as_of)
        valid, _ = transform_silver(_read_raw(paths))
        gold_aggregation(valid).write.mode("overwrite").saveAsTable(T_BATCH_GOLD)
        seconds = time.time() - t0
        rows = _read_raw(paths).count()
        record_bench("batch_full", as_of, len(paths), rows, seconds)
        print(f"  історія до {pd.Timestamp(as_of):%d.%m.%Y %H:%M}: {n_fmt(len(paths))} файлів, {n_fmt(rows)} рядків -> {seconds:.0f} с")


def _bench(mode):
    if not table_exists(T_BENCH):
        return pd.DataFrame()
    return (spark.table(T_BENCH).where(F.col("mode") == mode).orderBy("as_of").toPandas()
            .astype({"files_read": "int64", "rows_read": "int64", "seconds": "float64"}))


def plot_batch_cost():
    """Скільки коштує один повний перерахунок, коли історія росте."""
    b = _bench("batch_full")
    if b.empty:
        print("Немає замірів. Запустіть run_batch_benchmark().")
        return
    labels = [f"{t:%d.%m}" for t in pd.to_datetime(b["as_of"])]
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    bars = axes[0].bar(labels, b["rows_read"] / 1000, color="#c0392b")
    axes[0].bar_label(bars, labels=[f"{v/1000:.0f}k" for v in b["rows_read"]], padding=2, fontsize=9)
    axes[0].set_title("Рядків перечитано за один запуск", loc="left", fontweight="bold")
    axes[0].set_ylabel("тис. рядків")
    axes[0].set_xlabel("дані станом на")
    axes[1].plot(labels, b["seconds"], color="#c0392b", marker="o", linewidth=2)
    for x, v in zip(labels, b["seconds"]):
        axes[1].annotate(f"{v:.0f} с", (x, v), textcoords="offset points", xytext=(0, 7), ha="center", fontsize=9)
    axes[1].set_ylim(0, b["seconds"].max() * 1.3)
    axes[1].set_title("Тривалість одного запуску", loc="left", fontweight="bold")
    axes[1].set_ylabel("секунд")
    axes[1].set_xlabel("дані станом на")
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()
    last = b.iloc[-1]
    banner(f"💡 Кожен batch-запуск перечитує <b>всю</b> історію. Щоб бачити дані свіжістю в 1 годину, його довелося б запускати "
           f"24 рази на добу: це <b>{n_fmt(last.rows_read * 24)}</b> перечитаних рядків і близько "
           f"<b>{last.seconds * 24 / 60:.0f} хв</b> обчислень щодня, і з кожним днем більше.", color="#F2A900", bg="#fff8e1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Порівняння batch і streaming (після живого моніторингу)

# COMMAND ----------

def plot_batch_vs_streaming():
    b, s = _bench("batch_full"), _bench("stream_tick")
    if b.empty or s.empty:
        print("Потрібні і заміри batch (run_batch_benchmark), і хоча б кілька streaming-оновлень (live_monitoring).")
        return
    last_b = b.iloc[-1]
    s_rows, s_sec = float(s["rows_read"].median()), float(s["seconds"].median())
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))

    ax = axes[0]
    bars = ax.bar(["batch", "streaming"], [last_b.rows_read, s_rows], color=["#c0392b", "#27ae60"])
    ax.set_yscale("log")
    ax.bar_label(bars, labels=[n_fmt(last_b.rows_read), n_fmt(s_rows)], padding=2, fontsize=9)
    ax.set_title("Рядків за одне оновлення\n(логарифмічна шкала)", loc="left", fontweight="bold", fontsize=10)

    ax = axes[1]
    bars = ax.bar(["batch", "streaming"], [last_b.seconds, s_sec], color=["#c0392b", "#27ae60"])
    ax.bar_label(bars, labels=[f"{last_b.seconds:.0f} с", f"{s_sec:.0f} с"], padding=2, fontsize=9)
    ax.set_title("Секунд на одне оновлення", loc="left", fontweight="bold", fontsize=10)

    ax = axes[2]
    hours = np.arange(1, 25)
    ax.plot(hours, np.cumsum([last_b.rows_read] * 24) / 1e6, color="#c0392b", linewidth=2, label="batch щогодини")
    ax.plot(hours, np.cumsum([s_rows] * 24) / 1e6, color="#27ae60", linewidth=2, label="streaming")
    ax.set_title("Скільки рядків оброблено за добу,\nякщо оновлювати щогодини (млн)", loc="left", fontweight="bold", fontsize=10)
    ax.set_xlabel("година доби")
    ax.legend(frameon=False, fontsize=9)
    for a in axes:
        a.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()
    kpi_cards([
        ("Batch: рядків за оновлення", n_fmt(last_b.rows_read), "перечитує всю історію"),
        ("Streaming: рядків за оновлення", n_fmt(s_rows), "лише нова година"),
        ("У скільки разів менше даних", f"×{last_b.rows_read / max(s_rows, 1):,.0f}".replace(",", " "), "на кожному оновленні"),
        ("Batch: секунд", f"{last_b.seconds:.0f}", "і росте разом з історією"),
        ("Streaming: секунд", f"{s_sec:.0f}", "майже не залежить від історії"),
    ], accent="#27ae60")
    banner("ℹ️ На таких малих даних частину часу streaming-оновлення займає запуск самих запитів (фіксовані витрати Spark). "
           "Головна відмінність у тому, що обсяг роботи streaming <b>не росте</b> разом з історією, а batch росте лінійно.",
           color="#4C78A8", bg="#eef3f9")


def show_processing_diagram():
    """Наочно: скільки даних «перебирається» за одне оновлення в кожному підході."""
    b, s = _bench("batch_full"), _bench("stream_tick")
    if b.empty or s.empty:
        print("Потрібні заміри batch і streaming.")
        return
    full = float(b.iloc[-1].rows_read)
    new = float(s["rows_read"].median())
    state = float(RUNTIME.get("last_stream", {}).get("state_rows", 0))

    def bar(label, value, color, note):
        width = max(value / full * 100, 0.6)
        return (f"<div style='margin:10px 0;'><div style='font-size:13px;color:#1a1a1a;margin-bottom:3px;'><b>{label}</b> · {note}</div>"
                f"<div style='background:#eef0f3;border-radius:4px;height:26px;width:100%;'>"
                f"<div style='background:{color};height:26px;width:{width:.2f}%;border-radius:4px;'></div></div>"
                f"<div style='font-size:12px;color:#5f6b7a;margin-top:2px;'>{n_fmt(value)} рядків</div></div>")

    displayHTML(f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:820px;background:#ffffff;border:1px solid #e3e6ea;border-radius:8px;padding:12px 18px;">
      <div style="font-size:16px;font-weight:700;color:#1a1a1a;">Що відбувається всередині за одне оновлення</div>
      {bar("Batch", full, "#c0392b", "читає всі файли з початку історії, чистить і агрегує все заново")}
      {bar("Streaming: нові дані", new, "#27ae60", "Auto Loader бере лише файли, яких ще не бачив (checkpoint)")}
      {bar("Streaming: state у пам'яті", state, "#2e86c1", "проміжні суми відкритих годинних вікон; старі вікна прибирає watermark")}
    </div>""")
