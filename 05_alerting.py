# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Моніторинг і алертинг
# MAGIC
# MAGIC Ноутбук **не запускається окремо**: `06_demo` підтягує його через `%run`.
# MAGIC
# MAGIC Прототип системи алертингу платіжної компанії:
# MAGIC * `stream_update()`: одне оновлення = прийшла нова година даних → ingest → Gold → перевірка алертів;
# MAGIC * `live_monitoring()`: кілька оновлень поспіль, як у реальному часі;
# MAGIC * картка алерту в стилі Slack / PagerDuty: що сталося, наскільки це погано, скільки грошей під загрозою, що робити;
# MAGIC * порівняння часу виявлення та втрат: streaming проти нічного batch-звіту.

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def _seg_cond(values):
    return " AND ".join(f"{d} = '{values[d]}'" for d in CONFIG["dims"])


def _segment_label(a):
    if set(CONFIG["dims"]) == {"channel_id", "card_country_grp"}:
        return f"канал {a['channel_id']} · картки {a['card_country_grp']}"
    return " · ".join(f"{d}={a[d]}" for d in CONFIG["dims"])


def _scenario_segment():
    return {"channel_id": SCENARIO["channel_id"], "card_country_grp": SCENARIO["card_country"]}


def _amount_case():
    return "CASE bin_amount_usd " + " ".join(f"WHEN '{k}' THEN {v}" for k, v in AMOUNT_MID.items()) + " ELSE 25 END"


def _rows_in_bronze(files):
    return (spark.table(T_BRONZE)
            .where(F.element_at(F.split("_source_file", "/"), -1).isin(files)).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Картка алерту

# COMMAND ----------

def _alert_context(a):
    """Деталі для картки: головні причини відмов і середня сума платежу в цьому сегменті й годині (з Silver)."""
    ws = pd.Timestamp(a["window_start"]).strftime("%Y-%m-%d %H:%M:%S")
    we = pd.Timestamp(a["window_end"]).strftime("%Y-%m-%d %H:%M:%S")
    where = f"event_time >= TIMESTAMP'{ws}' AND event_time < TIMESTAMP'{we}' AND {_seg_cond(a)}"
    reasons = spark.sql(f"""SELECT COALESCE(error_message, '(без причини)') AS reason, SUM(count_orders) AS n
                            FROM {T_SILVER} WHERE {where} AND status = 'fail'
                            GROUP BY 1 ORDER BY 2 DESC LIMIT 3""").toPandas()
    avg_amount = spark.sql(f"""SELECT SUM(count_orders * {_amount_case()}) / SUM(count_orders)
                               FROM {T_SILVER} WHERE {where}""").first()[0]
    total_fail = reasons["n"].sum() if len(reasons) else 0
    return {"reasons": [(r.reason, 100 * r.n / total_fail) for r in reasons.itertuples()] if total_fail else [],
            "avg_amount": float(avg_amount or 25)}


def render_alert_card(a):
    ctx = _alert_context(a)
    p1 = a["severity"] == "P1"
    color, bg = ("#c0392b", "#fdecea") if p1 else ("#d35400", "#fef3e7")
    ws, we = pd.Timestamp(a["window_start"]), pd.Timestamp(a["window_end"])
    lost = int(a["lost_orders_est"] or 0)
    money = lost * ctx["avg_amount"]
    ticket = f"INC-{ws:%m%d}-{a['channel_id']}{a['card_country_grp']}"
    reasons = "".join(f"<li>{r} — <b>{s:.0f}%</b> відмов</li>" for r, s in ctx["reasons"])

    def metric(label, value, red=False):
        c = color if red else "#1a1a1a"
        return (f"<div style='flex:1;min-width:120px;background:#ffffff;border-radius:6px;padding:8px 10px;border:1px solid #f0d0cc;'>"
                f"<div style='font-size:11px;color:#6b7785;'>{label}</div>"
                f"<div style='font-size:20px;font-weight:700;color:{c};'>{value}</div></div>")

    displayHTML(f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:820px;background:{bg};border-left:8px solid {color};
                border-radius:8px;padding:14px 18px;color:#1a1a1a;margin:8px 0;box-shadow:0 1px 4px rgba(0,0,0,0.12);">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div style="font-size:13px;font-weight:700;color:{color};letter-spacing:0.5px;">🚨 {a['severity']} · ALERT · {a['alert_id']}</div>
        <div style="font-size:12px;color:#6b7785;">спрацював {pd.Timestamp(a['detected_at']):%d.%m.%Y о %H:%M}</div>
      </div>
      <div style="font-size:21px;font-weight:700;margin:6px 0 2px 0;">Різко впав рівень схвалення платежів</div>
      <div style="font-size:14px;margin-bottom:10px;">Сегмент: <b>{_segment_label(a)}</b> · година {ws:%H:%M}–{we:%H:%M}</div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        {metric("Схвалення зараз", f"{a['approval_pct']:.0f}%", red=True)}
        {metric("Норма для цієї години", f"{a['baseline_pct']:.0f}%")}
        {metric("Падіння", f"−{a['drop_pp']:.0f} п.п.", red=True)}
        {metric("Замовлень за годину", n_fmt(a['total_orders']))}
        {metric("Втрачено платежів (оцінка)", n_fmt(lost), red=True)}
        {metric("Під загрозою", f"≈ ${n_fmt(money)}", red=True)}
      </div>
      <div style="display:flex;gap:24px;flex-wrap:wrap;margin-top:12px;font-size:13px;">
        <div style="flex:1;min-width:240px;"><b>Головні причини відмов:</b><ul style="margin:4px 0 0 18px;padding:0;">{reasons}</ul></div>
        <div style="flex:1;min-width:300px;"><b>Що робити зараз:</b><ol style="margin:4px 0 0 18px;padding:0;">
          <li>Перевірити статус еквайра та процесингу для цього сегмента.</li>
          <li>Якщо збій на боці еквайра чи банку, перемкнути маршрутизацію на резервного еквайра.</li>
          <li>Призупинити автоматичні повторні списання (retries) для сегмента: зараз вони лише додадуть відмов.</li>
          <li>Повідомити account-менеджера мерчанта каналу {a['channel_id']}.</li></ol></div>
      </div>
      <div style="margin-top:12px;font-size:12px;color:#6b7785;border-top:1px solid #f0d0cc;padding-top:8px;">
        📣 Сповіщено: <b>#payments-oncall</b> (Slack) · черговий інженер (PagerDuty) · відкрито інцидент <b>{ticket}</b>
        &nbsp;<i>(симуляція)</i> · z-score = {a['z_score']}
      </div>
    </div>""")


def render_followup(a):
    ws, we = pd.Timestamp(a["window_start"]), pd.Timestamp(a["window_end"])
    ticket = f"INC-{ws:%m%d}-{a['channel_id']}{a['card_country_grp']}"
    banner(f"🔁 <b>Інцидент триває</b> ({_segment_label(a)}) · {ws:%H:%M}–{we:%H:%M}: схвалення <b>{a['approval_pct']:.0f}%</b> "
           f"(норма {a['baseline_pct']:.0f}%), ще ≈ <b>{n_fmt(a['lost_orders_est'] or 0)}</b> втрачених платежів. "
           f"Оновлення додано в інцидент {ticket}, повторних сповіщень не надсилаємо.", color="#c0392b", bg="#fdecea")


def _is_followup(a):
    prev = pd.Timestamp(a["window_start"]) - pd.Timedelta(hours=1)
    return spark.sql(f"""SELECT 1 FROM {T_ALERTS}
                         WHERE window_start = TIMESTAMP'{prev:%Y-%m-%d %H:%M:%S}' AND {_seg_cond(a)} LIMIT 1""").count() > 0


def show_alerts(alerts_pdf):
    for _, a in alerts_pdf.iterrows():
        render_followup(a) if _is_followup(a) else render_alert_card(a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Оновлення потоку

# COMMAND ----------

def _segment_rate_now(now):
    seg = _scenario_segment()
    r = spark.sql(f"""SELECT approval_rate FROM {T_GOLD}
                      WHERE window_end = TIMESTAMP'{pd.Timestamp(now):%Y-%m-%d %H:%M:%S}' AND {_seg_cond(seg)}""").first()
    return None if r is None or r[0] is None else 100 * r[0]


def stream_update(until=None, record=True, show=True):
    """Одне оновлення: прийшли нові дані (наступна година або все до `until`) → ingest → Gold → алерти."""
    t0 = time.time()
    files = release_until(until) if until is not None else release_next_hour()
    now = clock()
    ing = run_ingest()
    gold = run_gold()
    new_alerts = detect_alerts(now)
    seconds = time.time() - t0
    if files and ing["rows"] == 0:                 # запасний варіант, якщо метрики потоку недоступні
        ing["rows"] = _rows_in_bronze(files)
    RUNTIME["last_stream"] = {**gold, "rows": ing["rows"], "seconds": seconds, "now": str(now)}
    if record and files:
        record_bench("stream_tick", now, len(files), ing["rows"], seconds)
    if show:
        ok = len(new_alerts) == 0
        color, bg, icon, msg = (("#27ae60", "#eefaf1", "✅", "відхилень немає") if ok
                                else ("#c0392b", "#fdecea", "🚨", f"нових алертів: {len(new_alerts)}"))
        seg_rate = _segment_rate_now(now)
        seg_txt = f" · {_segment_label(_scenario_segment())}: <b>{seg_rate:.0f}%</b>" if seg_rate is not None else ""
        what = (f"прийшла година {now - pd.Timedelta(hours=1):%H:%M}–{now:%H:%M}" if len(files) == 1
                else f"прийшло файлів: {n_fmt(len(files))}")
        banner(f"🕐 <b>{now:%d.%m.%Y %H:%M}</b> · {what}: <b>{n_fmt(ing['rows'])}</b> рядків · обробка <b>{seconds:.1f} с</b> · "
               f"state: <b>{n_fmt(gold['state_rows'])}</b> рядків{seg_txt} · {icon} {msg}", color=color, bg=bg)
        if not ok:
            show_alerts(new_alerts)
    return {"now": now, "files": files, "ingest": ing, "gold": gold, "alerts": new_alerts, "seconds": seconds}


def warm_up(until=None):
    """Підготовка: прогнати через streaming усю історію до початку живого демо (одним великим оновленням)."""
    until = pd.Timestamp(until or f"{SCENARIO['day']} {SCENARIO['live_from_hour']:02d}:00")
    t0 = time.time()
    r = stream_update(until=until, record=False, show=False)
    banner(f"✅ Історію завантажено: {n_fmt(len(r['files']))} погодинних файлів, {n_fmt(r['ingest']['rows'])} рядків, "
           f"{time.time() - t0:.0f} с. Змодельований час: <b>{r['now']:%d.%m.%Y %H:%M}</b>.", color="#27ae60", bg="#eefaf1")


def live_monitoring(hours=4):
    """Живий моніторинг: щоразу приходить наступна година даних."""
    banner(f"▶️ Моніторинг запущено. Зараз <b>{clock():%d.%m.%Y %H:%M}</b>, оновлення щогодини, "
           f"сегменти: {' × '.join(CONFIG['dims'])}.", color="#4C78A8", bg="#eef3f9")
    return [stream_update() for _ in range(hours)]


def catch_up(until=None):
    """Швидко «перемотати» час уперед (до ранку наступного дня), одним оновленням і без карток."""
    until = pd.Timestamp(until or pd.Timestamp(SCENARIO["day"]) + pd.Timedelta(days=1, hours=SCENARIO["batch_report_hour"] + 2))
    r = stream_update(until=until, record=False, show=False)
    n = len(r["alerts"])
    banner(f"⏩ Перемотали до <b>{r['now']:%d.%m.%Y %H:%M}</b>: оброблено {n_fmt(len(r['files']))} годин, "
           f"додано алертів: {n} (продовження того самого інциденту).", color="#4C78A8", bg="#eef3f9")
    if n:
        display(r["alerts"][["alert_id", "window_start", "approval_pct", "baseline_pct", "drop_pp", "lost_orders_est", "severity"]])


def rerun_without_new_data():
    """Доказ, що це streaming: запускаємо ті самі запити без нових файлів."""
    t0 = time.time()
    ing, gold = run_ingest(), run_gold()
    banner(f"🔁 Повторний запуск без нових файлів: ingest прочитав <b>{n_fmt(ing['rows'])}</b> рядків, Gold <b>{n_fmt(gold['rows'])}</b> "
           f"({time.time() - t0:.1f} с). Checkpoint пам'ятає, що вже оброблено. Batch на цьому місці перечитав би всю історію.",
           color="#4C78A8", bg="#eef3f9")

# COMMAND ----------

# MAGIC %md
# MAGIC ## День інциденту: графік, час виявлення, втрати

# COMMAND ----------

def _segment_hourly(day=None):
    """Для сегмента зі сценарію: замовлення, успішні, схвалення по годинах дня + норма (той самий день тижня, 4 тижні)."""
    day = pd.Timestamp(day or SCENARIO["day"])
    cond = _seg_cond(_scenario_segment())
    d = f"{day:%Y-%m-%d}"
    return spark.sql(f"""
        WITH cur AS (
            SELECT hour(window_start) AS h, total_orders AS tot, success_orders AS ok
            FROM {T_GOLD} WHERE {cond} AND CAST(window_start AS DATE) = DATE'{d}'
        ),
        base AS (
            SELECT hour(window_start) AS h, SUM(success_orders) / SUM(total_orders) AS base_rate
            FROM {T_GOLD}
            WHERE {cond} AND window_start >= DATE'{d}' - INTERVAL {7 * CONFIG['baseline_weeks']} DAYS
              AND window_start < DATE'{d}' AND dayofweek(window_start) = dayofweek(DATE'{d}')
            GROUP BY 1
        )
        SELECT b.h, c.tot, c.ok, c.ok / c.tot AS rate, b.base_rate
        FROM base b LEFT JOIN cur c ON b.h = c.h ORDER BY b.h
    """).toPandas().astype({"h": "int64", "tot": "float64", "ok": "float64", "rate": "float64", "base_rate": "float64"})


def plot_incident_day(day=None):
    s = SCENARIO
    day = pd.Timestamp(day or s["day"])
    h = _segment_hourly(day)
    al = spark.sql(f"""SELECT hour(window_start) AS h FROM {T_ALERTS}
                       WHERE {_seg_cond(_scenario_segment())} AND CAST(window_start AS DATE) = DATE'{day:%Y-%m-%d}'""").toPandas()
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.axvspan(s["from_hour"], s["to_hour"], color="#e74c3c", alpha=0.08, label="інцидент (змодельований)")
    ax.plot(h["h"] + 0.5, h["base_rate"] * 100, color="#7f8c8d", linestyle="--", linewidth=1.5, label="норма: та сама година, 4 попередні четверги")
    ax.plot(h["h"] + 0.5, h["rate"] * 100, color="#2e86c1", marker="o", linewidth=2, label="фактичне схвалення")
    if len(al):
        pts = h[h["h"].isin(al["h"])]
        ax.scatter(pts["h"] + 0.5, pts["rate"] * 100, s=140, color="#c0392b", zorder=5, label="алерт")
        first = int(al["h"].min())
        y = float(h.loc[h["h"] == first, "rate"].iloc[0] * 100)
        ax.annotate(f"перший алерт о {first + 1:02d}:00\n(через {first + 1 - s['from_hour']} год після початку)",
                    (first + 0.5, y), xytext=(first + 3.2, max(y - 22, 4)), fontsize=9, color="#c0392b",
                    arrowprops=dict(arrowstyle="->", color="#c0392b"))
    now = clock()
    if now is not None and now.normalize() == day.normalize():
        ax.axvline(now.hour, color="#333", linewidth=1)
        ax.text(now.hour + 0.1, 96, f"зараз {now:%H:%M}", fontsize=8)
    ax.set_xlim(0, 24)
    ax.set_ylim(0, 100)
    ax.set_xticks(range(0, 25, 2))
    ax.set_xlabel("година доби")
    ax.set_ylabel("рівень схвалення, %")
    ax.set_title(f"{_segment_label(_scenario_segment()).capitalize()}, {day:%d.%m.%Y}: схвалення по годинах", loc="left", fontweight="bold")
    ax.legend(frameon=False, fontsize=8, loc="lower left")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()


def _first_stream_detection(day):
    r = spark.sql(f"""SELECT MIN(detected_at) FROM {T_ALERTS}
                      WHERE {_seg_cond(_scenario_segment())} AND CAST(window_start AS DATE) = DATE'{day:%Y-%m-%d}'""").first()[0]
    return None if r is None else pd.Timestamp(r)


def _impact():
    s = SCENARIO
    day = pd.Timestamp(s["day"])
    inc_start, inc_end = day + pd.Timedelta(hours=s["from_hour"]), day + pd.Timedelta(hours=s["to_hour"])
    stream_detect = _first_stream_detection(day)
    batch_detect = day + pd.Timedelta(days=1, hours=s["batch_report_hour"])
    react = pd.Timedelta(minutes=s["reaction_minutes"])
    h = _segment_hourly(day)
    h = h[(h["h"] >= s["from_hour"]) & (h["h"] < s["to_hour"])].copy()
    h["lost"] = (h["tot"] * h["base_rate"] - h["ok"]).clip(lower=0)
    avg = spark.sql(f"""SELECT SUM(count_orders * {_amount_case()}) / SUM(count_orders) FROM {T_SILVER}
                        WHERE {_seg_cond(_scenario_segment())} AND CAST(event_time AS DATE) = DATE'{day:%Y-%m-%d}'""").first()[0]
    avg = float(avg or 25)

    def loss_until(fix):
        starts = day + pd.to_timedelta(h["h"], unit="h")
        frac = ((fix - starts) / pd.Timedelta(hours=1)).clip(lower=0, upper=1)
        return float((h["lost"] * frac).sum())

    return {"day": day, "inc_start": inc_start, "inc_end": inc_end, "stream_detect": stream_detect, "batch_detect": batch_detect,
            "react": react, "hourly": h, "avg_amount": avg,
            "loss_stream": loss_until(stream_detect + react) if stream_detect is not None else None,
            "loss_batch": loss_until(batch_detect + react)}


def show_detection_timeline():
    i = _impact()
    if i["stream_detect"] is None:
        print("Streaming-алерту для сценарію ще немає: спершу запустіть live_monitoring().")
        return
    h = i["hourly"]
    times = [i["inc_start"]] + list(i["day"] + pd.to_timedelta(h["h"] + 1, unit="h"))
    cum = [0] + list(h["lost"].cumsum())
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.axvspan(i["inc_start"], i["inc_end"], color="#e74c3c", alpha=0.08)
    ax.step(times, cum, where="post", color="#c0392b", linewidth=2, label="накопичені втрати (платежі)")
    ax.fill_between(times, cum, step="post", color="#c0392b", alpha=0.12)
    top = max(cum) * 1.35
    for t, label, color in [(i["stream_detect"], "streaming-алерт", "#27ae60"), (i["batch_detect"], "нічний batch-звіт", "#c0392b")]:
        delay = (t - i["inc_start"]).total_seconds() / 3600
        ax.axvline(t, color=color, linewidth=2.5)
        ax.text(t, top * 0.93, f" {label}\n {t:%d.%m %H:%M}\n (+{delay:.0f} год)", color=color, fontsize=9, va="top", fontweight="bold")
    ax.axvline(i["stream_detect"] + i["react"], color="#27ae60", linestyle=":", linewidth=1.5)
    ax.set_xlim(i["day"] + pd.Timedelta(hours=6), i["batch_detect"] + pd.Timedelta(hours=2))
    ax.set_ylim(0, top)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m\n%H:%M"))
    ax.set_ylabel("втрачених платежів")
    ax.set_title("Коли команда дізнається про проблему", loc="left", fontweight="bold")
    ax.legend(frameon=False, fontsize=9, loc="center right")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()


def show_business_impact():
    i = _impact()
    if i["loss_stream"] is None:
        print("Streaming-алерту для сценарію ще немає: спершу запустіть live_monitoring().")
        return
    d_stream = (i["stream_detect"] - i["inc_start"]).total_seconds() / 3600
    d_batch = (i["batch_detect"] - i["inc_start"]).total_seconds() / 3600
    ls, lb, avg = i["loss_stream"], i["loss_batch"], i["avg_amount"]

    fig, ax = plt.subplots(figsize=(7, 3.6))
    bars = ax.barh(["streaming", "нічний batch"], [ls, lb], color=["#27ae60", "#c0392b"])
    ax.bar_label(bars, labels=[f"{n_fmt(ls)} платежів ≈ ${n_fmt(ls * avg)}", f"{n_fmt(lb)} платежів ≈ ${n_fmt(lb * avg)}"],
                 padding=4, fontsize=10)
    ax.set_xlim(0, lb * 1.6)
    ax.set_title("Втрачені платежі за один інцидент (оцінка)", loc="left", fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()

    kpi_cards([
        ("Streaming: сигнал через", f"{d_stream:.0f} год", f"о {i['stream_detect']:%H:%M} того ж дня"),
        ("Batch: сигнал через", f"{d_batch:.0f} год", f"о {i['batch_detect']:%H:%M} наступного дня"),
        ("Збережено платежів", n_fmt(lb - ls), f"{(1 - ls / lb) * 100:.0f}% втрат вдалося б уникнути" if lb else ""),
        ("Збережено грошей", f"≈ ${n_fmt((lb - ls) * avg)}", f"середній чек ≈ ${avg:.0f}"),
    ], accent="#27ae60")
    banner(f"ℹ️ Як рахуємо: втрата за годину = замовлення × норма схвалення − фактично успішні. Припускаємо, що після сигналу команда "
           f"за {SCENARIO['reaction_minutes']} хв перемикає трафік на резервного еквайра. Batch-звіт приходить, коли інцидент "
           "уже скінчився, тож усі втрати вже сталися. Це оцінка на змодельованому інциденті, а не фактичні гроші компанії.",
           color="#4C78A8", bg="#eef3f9")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Додатково: пізні дані і таблиця алертів

# COMMAND ----------

def demo_late_data(day="2023-04-03", hour=12):
    """Дані за квітень приходять у вересні: Bronze і Silver їх збережуть, а Gold проігнорує завдяки watermark."""
    before = spark.table(T_GOLD).agg(F.sum("total_orders")).first()[0]
    name = release_late(day, hour)
    ing, gold = run_ingest(), run_gold()
    after = spark.table(T_GOLD).agg(F.sum("total_orders")).first()[0]
    banner(f"📦 Надійшов файл <b>{name}</b> (події {day}, тобто на місяці старші за watermark).<br>"
           f"Ingest прочитав <b>{n_fmt(ing['rows'])}</b> рядків і зберіг їх у Bronze та Silver. "
           f"Gold відкинув як запізнілі: <b>{n_fmt(gold['dropped_late'])}</b> рядків.<br>"
           f"Замовлень у Gold: {n_fmt(before)} → {n_fmt(after)} (без змін). Закриті вікна не переписуються.",
           color="#8e44ad", bg="#f5eef8")


def show_alerts_table(limit=20):
    _ensure_alerts_table()
    display(spark.sql(f"""
        SELECT alert_id, severity, window_start AS `година`, channel_id AS `канал`, card_country_grp AS `країна`,
               total_orders AS `замовлень`, approval_pct AS `схвалення_%`, baseline_pct AS `норма_%`,
               drop_pp AS `падіння_пп`, z_score, lost_orders_est AS `втрачено_оцінка`, detected_at
        FROM {T_ALERTS} ORDER BY window_start DESC LIMIT {limit}"""))
