# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · EDA: функції з графіками
# MAGIC
# MAGIC Ноутбук **не запускається окремо**: `06_demo` підтягує його через `%run` і викликає функції `eda_*()`.
# MAGIC
# MAGIC Усі обчислення робить **Spark** (SQL над Delta-таблицею), у pandas переходять лише маленькі агрегати для графіків.
# MAGIC
# MAGIC Важливо про дані: один рядок = група однакових спроб оплати, а `count_orders` каже, скільки їх.
# MAGIC Тому всі частки **зважені на `count_orders`**. *Рівень схвалення (approval rate)* = успішні / (успішні + відхилені).

# COMMAND ----------

import matplotlib.pyplot as plt

C_BLUE, C_RED, C_GREEN, C_ORANGE = "#4C78A8", "#E45756", "#54A24B", "#F58518"
C_TEAL, C_PURPLE, C_PINK, C_GREY = "#72B7B2", "#B279A2", "#FF9DA6", "#9D9D9D"


def eda_prepare(force=False):
    """Один раз читаємо CSV Spark-ом, приводимо типи й зберігаємо як Delta-таблицю: далі всі графіки рахуються швидко."""
    if table_exists(T_EDA) and not force:
        print(f"✅ Таблиця для EDA вже є: {T_EDA}")
        return
    raw = spark.read.option("header", True).option("sep", ";").csv(RAW_CSV)
    raw = raw.select([c for c in raw.columns if not c.startswith("_c")])        # прибираємо порожню останню колонку
    # try_* повертають NULL замість помилки (serverless працює в ANSI-режимі)
    typed = raw.select(
        F.expr("try_to_date(substr(created_day, 1, 10), 'yyyy-MM-dd')").alias("day"),
        "channel_id", "order_type", "payment_source", "merchant_token_type", "status", "error_code", "error_message",
        "bin_amount_usd", "card_brand", "card_type", "card_country", "ip_country", "retry_number", "created_day",
        F.expr("try_cast(is_secured AS BOOLEAN)").alias("is_secured"),
        F.expr("try_cast(count_orders AS INT)").alias("orders"),
    )
    typed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_EDA)
    print(f"✅ Створено {T_EDA}: {n_fmt(spark.table(T_EDA).count())} рядків")


def _approval_by(expr, where="1=1"):
    """Spark SQL: замовлення та рівень схвалення для кожного значення `expr`."""
    return spark.sql(f"""
        SELECT {expr} AS segment,
               SUM(orders) AS orders,
               ROUND(100 * COALESCE(SUM(CASE WHEN status = 'success' THEN orders END), 0) / SUM(orders), 1) AS approval_pct
        FROM {T_EDA}
        WHERE status IN ('success', 'fail') AND {where}
        GROUP BY 1
    """).toPandas().astype({"orders": "int64", "approval_pct": "float64"})


def _bar(df, title, color=C_BLUE, xlabel="", rotate=0, labels=None, avg=None):
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(df["segment"].astype(str), df["approval_pct"], color=color)
    ax.bar_label(bars, labels=labels or [f"{v:.0f}%" for v in df["approval_pct"]], padding=2, fontsize=9)
    if avg is not None:
        ax.axhline(avg, color=C_GREY, linestyle="--", linewidth=1)
        ax.text(len(df) - 0.5, avg + 1.5, f"в середньому {avg:.0f}%", ha="right", fontsize=8, color="#555")
    ax.set_title(title, loc="left", fontweight="bold")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("рівень схвалення, %")
    ax.set_ylim(0, 105)
    ax.spines[["top", "right"]].set_visible(False)
    plt.xticks(rotation=rotate)
    plt.tight_layout()
    plt.show()


def _overall_rate():
    return float(_approval_by("'all'").approval_pct[0])


def _say(text):
    banner(f"💡 {text}", color="#F2A900", bg="#fff8e1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Огляд і тренд

# COMMAND ----------

def eda_overview():
    o = spark.sql(f"""
        SELECT COUNT(*) AS n_rows, SUM(orders) AS n_orders, MIN(day) AS first_day, MAX(day) AS last_day,
               ROUND(100 * SUM(CASE WHEN status = 'success' THEN orders END)
                     / SUM(CASE WHEN status IN ('success', 'fail') THEN orders END), 1) AS approval_pct,
               SUM(CASE WHEN day IS NULL THEN 1 ELSE 0 END) AS bad_dates,
               SUM(CASE WHEN card_brand IS NULL OR card_country IS NULL OR card_type IS NULL THEN 1 ELSE 0 END) AS missing_card,
               COUNT(DISTINCT card_country) AS n_countries
        FROM {T_EDA}""").first()
    kpi_cards([
        ("Рядків", n_fmt(o.n_rows), "агреговані спроби оплати"),
        ("Замовлень", n_fmt(o.n_orders), "з урахуванням count_orders"),
        ("Рівень схвалення", f"{o.approval_pct}%", "успішні / (успішні + відхилені)"),
        ("Період", f"{o.first_day:%d.%m} – {o.last_day:%d.%m.%Y}", "6 місяців"),
        ("Країн карток", str(o.n_countries), ""),
        ("Неіснуючі дати", n_fmt(o.bad_dates), "напр. 31.09 → карантин"),
    ])
    _say(f"Майже кожен другий платіж відхиляється: схвалюється лише {o.approval_pct}%. "
         f"{n_fmt(o.bad_dates)} рядків мають дати, яких не існує, а {n_fmt(o.missing_card)} не мають даних про картку.")


def eda_monthly_trend():
    m = spark.sql(f"""
        SELECT date_format(day, 'yyyy-MM') AS month, SUM(orders) AS orders,
               100 * SUM(CASE WHEN status = 'success' THEN orders END) / SUM(orders) AS approval_pct
        FROM {T_EDA} WHERE status IN ('success', 'fail') AND day IS NOT NULL
        GROUP BY 1 ORDER BY 1""").toPandas().astype({"orders": "int64", "approval_pct": "float64"})
    names = {"04": "квіт", "05": "трав", "06": "черв", "07": "лип", "08": "серп", "09": "вер"}
    m["label"] = m["month"].str[5:7].map(names).fillna(m["month"])

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(m["label"], m["orders"] / 1000, color="#c9d7ea", label="замовлень, тис.")
    ax.bar_label(bars, labels=[f"{v/1000:.0f}k" for v in m["orders"]], padding=2, fontsize=9)
    ax.set_ylabel("замовлень, тис.")
    ax.set_ylim(0, m["orders"].max() / 1000 * 1.3)
    ax2 = ax.twinx()
    ax2.plot(m["label"], m["approval_pct"], color=C_RED, marker="o", linewidth=2, label="схвалення, %")
    for x, v in zip(m["label"], m["approval_pct"]):
        ax2.annotate(f"{v:.0f}%", (x, v), textcoords="offset points", xytext=(0, 7), ha="center", fontsize=9, color=C_RED)
    ax2.set_ylim(0, 100)
    ax2.set_ylabel("рівень схвалення, %")
    ax.set_title("Обсяг росте, а частка схвалених платежів стабільна", loc="left", fontweight="bold")
    ax.spines[["top"]].set_visible(False)
    plt.tight_layout()
    plt.show()
    growth = (m["orders"].iloc[-1] / m["orders"].iloc[0] - 1) * 100
    _say(f"За пів року обсяг зріс на {growth:.0f}%, а рівень схвалення тримається в межах "
         f"{m['approval_pct'].min():.0f}–{m['approval_pct'].max():.0f}%. Даних стає більше, тож перераховувати все щоразу буде все дорожче.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Чому платежі не проходять

# COMMAND ----------

def eda_retries():
    r = _approval_by("CASE WHEN retry_number = '0' THEN 'перша спроба' ELSE concat('повтор ', retry_number) END",
                     where="retry_number IN ('0', '1', '2', '3', '4')")
    r = r.set_index("segment").loc[["перша спроба", "повтор 1", "повтор 2", "повтор 3", "повтор 4"]].reset_index()
    _bar(r, "Списання за підпискою: перша спроба проти повторних", color=[C_BLUE] + [C_RED] * 4)
    s = spark.sql(f"""
        SELECT 100 * SUM(CASE WHEN retry_number IN ('1','2','3','4') THEN orders END) / SUM(orders) AS att,
               100 * SUM(CASE WHEN retry_number IN ('1','2','3','4') AND status = 'success' THEN orders END)
                   / SUM(CASE WHEN status = 'success' THEN orders END) AS succ
        FROM {T_EDA} WHERE status IN ('success', 'fail')""").first()
    _say(f"Перша спроба проходить у {r.approval_pct[0]:.0f}% випадків, повторні лише в {r.approval_pct[1:].min():.0f}–{r.approval_pct[1:].max():.0f}%. "
         f"Повтори становлять {s.att:.0f}% усіх спроб, але дають лише {s.succ:.0f}% успішних платежів.")


def eda_decline_reasons(top=8):
    reasons = spark.sql(f"""
        SELECT COALESCE(error_message, '(без причини)') AS reason, SUM(orders) AS failed
        FROM {T_EDA} WHERE status = 'fail' GROUP BY 1 ORDER BY 2 DESC""").toPandas().astype({"failed": "int64"})
    reasons["share"] = 100 * reasons["failed"] / reasons["failed"].sum()
    t = reasons.head(top).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(t["reason"], t["share"], color=C_RED)
    ax.bar_label(bars, labels=[f"{v:.0f}%" for v in t["share"]], padding=2, fontsize=9)
    ax.set_title("Причини відмов (частка від усіх відхилених замовлень)", loc="left", fontweight="bold")
    ax.set_xlabel("% відхилених замовлень")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()
    _say(f"Три причини дають {reasons.share.head(3).sum():.0f}% усіх відмов: «{reasons.reason[0]}», «{reasons.reason[1]}», «{reasons.reason[2]}». "
         "Більшість відмов приходить від банку клієнта, а не через технічні збої.")


def eda_amount():
    order = ["[0,10)", "[10,20)", "[20,40)", "[40,60)", "[60,100)", "[100,150)"]
    a = _approval_by("bin_amount_usd").set_index("segment").reindex(order).dropna().reset_index()
    a = a[a["orders"] >= 500]
    _bar(a, "Що більша сума, то рідше платіж проходить", color=C_GREEN, xlabel="сума, USD")
    _say(f"Платежі до $10 схвалюються в {a.approval_pct.iloc[0]:.0f}% випадків, а на $40–60 лише в "
         f"{a.set_index('segment').approval_pct.get('[40,60)', float('nan')):.0f}%.")


def eda_3ds():
    s = _approval_by("CASE WHEN is_secured THEN 'з 3-D Secure' ELSE 'без 3-D Secure' END", where="is_secured IS NOT NULL")
    share = s.set_index("segment").orders.get("з 3-D Secure", 0) / s.orders.sum() * 100
    _bar(s.sort_values("approval_pct"), "Платежі з 3-D Secure проходять значно рідше", color=C_ORANGE)
    _say(f"Через 3-D Secure проходить лише {share:.0f}% замовлень, і вони найважчі. Обережно з висновками: "
         "3DS, найімовірніше, вмикають для ризикованіших платежів, тож це зв'язок, а не доказ причини.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Хто платить: канали, способи оплати, країни

# COMMAND ----------

def eda_channels():
    c = _approval_by("channel_id").sort_values("segment")
    c["share"] = 100 * c["orders"] / c["orders"].sum()
    labels = [f"{a:.0f}%\n({s:.0f}% обсягу)" for a, s in zip(c["approval_pct"], c["share"])]
    _bar(c, "Рівень схвалення за каналами продажу", color=C_TEAL, xlabel="канал", labels=labels, avg=_overall_rate())
    big = c.sort_values("orders", ascending=False).head(2)
    _say(f"Два найбільші канали ({', '.join(big.segment)}) дають {big.share.sum():.0f}% обсягу, і саме в них схвалення найнижче. "
         "Тому моніторимо кожен канал окремо, а не одне загальне число.")


def eda_payment_methods():
    names = {"card": "картка", "apple-pay": "Apple Pay", "google-pay": "Google Pay"}
    p = _approval_by("payment_source")
    p["share"] = 100 * p["orders"] / p["orders"].sum()
    p["segment"] = p["segment"].map(names).fillna(p["segment"])
    p = p.sort_values("approval_pct")
    labels = [f"{a:.0f}%\n({s:.0f}% обсягу)" for a, s in zip(p["approval_pct"], p["share"])]
    _bar(p, "Рівень схвалення за способом оплати", color=C_PURPLE, labels=labels)
    _say(f"Apple Pay проходить найкраще ({p.set_index('segment').approval_pct.get('Apple Pay', float('nan')):.0f}%), "
         f"Google Pay найгірше ({p.set_index('segment').approval_pct.get('Google Pay', float('nan')):.0f}%).")


def eda_countries(top=12):
    c = _approval_by("card_country", where="card_country IS NOT NULL")
    c = c.sort_values("orders", ascending=False).head(top).sort_values("approval_pct")
    avg = _overall_rate()
    colors = [C_RED if v < avg else C_BLUE for v in c["approval_pct"]]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bars = ax.barh(c["segment"], c["approval_pct"], color=colors)
    ax.bar_label(bars, labels=[f"{a:.0f}%  ({n_fmt(o)})" for a, o in zip(c["approval_pct"], c["orders"])], padding=2, fontsize=8)
    ax.axvline(avg, color=C_GREY, linestyle="--", linewidth=1)
    ax.set_xlim(0, 105)
    ax.set_title(f"Топ-{top} країн карток за обсягом: рівень схвалення (у дужках замовлення)", loc="left", fontweight="bold")
    ax.set_xlabel("рівень схвалення, %  (пунктир: середнє)")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()
    usa = c.set_index("segment").loc["USA"] if "USA" in set(c.segment) else None
    lo, hi = c.iloc[0], c.iloc[-1]
    _say(f"Різниця між країнами велика: від {lo.approval_pct:.0f}% ({lo.segment}) до {hi.approval_pct:.0f}% ({hi.segment}). "
         + (f"США дають {usa.orders / c.orders.sum() * 100:.0f}% обсягу цього топу, тому збій там б'є найсильніше." if usa is not None else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Тижневий ритм (саме він визначає логіку алертів)

# COMMAND ----------

def eda_weekday():
    wd = spark.sql(f"""
        SELECT dayofweek(day) AS dow, SUM(orders) AS orders,
               100 * SUM(CASE WHEN status = 'success' THEN orders END) / SUM(orders) AS approval_pct
        FROM {T_EDA} WHERE status IN ('success', 'fail') AND day IS NOT NULL
        GROUP BY 1""").toPandas().astype({"dow": "int64", "orders": "int64", "approval_pct": "float64"})
    names = {2: "Пн", 3: "Вт", 4: "Ср", 5: "Чт", 6: "Пт", 7: "Сб", 1: "Нд"}
    wd["segment"] = wd["dow"].map(names)
    wd = wd.set_index("segment").loc[["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]].reset_index()

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(wd["segment"], wd["approval_pct"], color=[C_RED if s == "Ср" else C_BLUE for s in wd["segment"]])
    ax.bar_label(bars, labels=[f"{v:.0f}%" for v in wd["approval_pct"]], padding=2, fontsize=9)
    ax.set_ylim(0, 100)
    ax.set_ylabel("рівень схвалення, %")
    ax.spines[["top"]].set_visible(False)
    ax2 = ax.twinx()
    ax2.plot(wd["segment"], wd["orders"] / 1000, color="#333333", marker="o", linewidth=1.5)
    ax2.set_ylabel("замовлень, тис. (лінія)")
    ax2.set_ylim(0, wd["orders"].max() / 1000 * 1.3)
    ax.set_title("Схвалення (стовпці) та обсяг (лінія) за днями тижня", loc="left", fontweight="bold")
    plt.tight_layout()
    plt.show()
    w = wd.set_index("segment").approval_pct
    _say(f"У середу схвалюється лише {w['Ср']:.0f}% платежів, у вихідні понад {min(w['Сб'], w['Нд']):.0f}%. "
         "Вівторок і середа мають найбільший обсяг (масові списання за підписками) і найнижче схвалення.")


def eda_busy_days():
    daily = spark.sql(f"""
        SELECT day, SUM(orders) AS orders,
               100 * SUM(CASE WHEN status = 'success' THEN orders END) / SUM(orders) AS approval_pct
        FROM {T_EDA} WHERE status IN ('success', 'fail') AND day IS NOT NULL
        GROUP BY day ORDER BY day""").toPandas().astype({"orders": "int64", "approval_pct": "float64"})
    corr = daily["orders"].corr(daily["approval_pct"])
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.scatter(daily["orders"], daily["approval_pct"], s=18, color=C_BLUE, alpha=0.8)
    z = np.polyfit(daily["orders"], daily["approval_pct"], 1)
    xs = np.linspace(daily["orders"].min(), daily["orders"].max(), 50)
    ax.plot(xs, np.polyval(z, xs), color=C_RED, linewidth=1.5)
    ax.set_xlabel("замовлень за день")
    ax.set_ylabel("рівень схвалення, %")
    ax.set_title("Що завантаженіший день, то нижче схвалення", loc="left", fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    plt.show()
    _say(f"Кореляція між обсягом за день і рівнем схвалення: {corr:.2f}. "
         "Висновок для моніторингу: порівнювати сьогодні з «останніми 7 днями» не можна, бо тоді щосереди будуть хибні тривоги. "
         "Наші алерти порівнюють <b>ту саму годину того самого дня тижня</b> за попередні 4 тижні.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Швидкі факти

# COMMAND ----------

def eda_quick_facts():
    ct = _approval_by("card_type", where="card_type IS NOT NULL")
    ct = ct[ct["orders"] >= 5000].sort_values("approval_pct")
    _bar(ct, "Рівень схвалення за типом картки", color=C_TEAL)

    xb = _approval_by("CASE WHEN card_country <> ip_country THEN 'країна картки ≠ країна IP' ELSE 'та сама країна' END",
                      where="card_country IS NOT NULL AND ip_country IS NOT NULL")
    _bar(xb.sort_values("approval_pct"), "Транскордонні платежі", color=C_PURPLE)

    miss = _approval_by("CASE WHEN card_brand IS NULL OR card_country IS NULL OR card_type IS NULL "
                        "THEN 'даних картки немає' ELSE 'дані картки є' END")
    _bar(miss.sort_values("approval_pct"), "Платежі без даних про картку", color=C_PINK)
    _say("Передплачені (prepaid) картки й транскордонні платежі відхиляються частіше, а платежі без даних картки "
         "майже ніколи не проходять (імовірно, картку так і не вдалося зчитати).")


def eda_all():
    """Усі графіки EDA підряд."""
    for f in [eda_overview, eda_monthly_trend, eda_retries, eda_decline_reasons, eda_amount, eda_3ds,
              eda_channels, eda_payment_methods, eda_countries, eda_weekday, eda_busy_days, eda_quick_facts]:
        f()
