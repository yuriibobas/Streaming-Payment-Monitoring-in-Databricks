# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Налаштування та спільні функції
# MAGIC
# MAGIC Цей ноутбук **не запускається окремо**: його підтягує `06_demo` командою `%run`.
# MAGIC
# MAGIC Тут зібрано все, що залежить від конкретного датасету:
# MAGIC * шляхи у Volume і назви таблиць;
# MAGIC * `CONFIG`: які сегменти моніторимо, розмір вікна, watermark, пороги алертів;
# MAGIC * `SCENARIO`: змодельований інцидент для демо.
# MAGIC
# MAGIC Щоб застосувати пайплайн до інших даних, найперше змінюємо саме цей ноутбук.

# COMMAND ----------

import os, json, time, shutil, datetime
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# Працюємо в UTC, щоб годинні вікна починалися рівно о 00:00, 01:00, ...
try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception as e:
    print("Не вдалося встановити часовий пояс (некритично):", e)

CATALOG = spark.sql("SELECT current_catalog()").first()[0]     # у Free Edition зазвичай `workspace`
SCHEMA, VOLUME = "payments_streaming", "data"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Шляхи і таблиці
# MAGIC
# MAGIC * `staging/`: увесь датасет, нарізаний на **погодинні файли**. Це «архів подій», stream його не бачить.
# MAGIC * `landing/`: папка, за якою стежить stream. Функції `release_*` копіюють туди наступні години, і так ми імітуємо надходження нових даних.

# COMMAND ----------

BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_CSV = f"{BASE}/BillingOps-data.csv"      # файл, який ви завантажуєте у Volume
STAGING = f"{BASE}/staging"                  # усі погодинні файли (архів)
LANDING = f"{BASE}/landing"                  # сюди «надходять» нові дані
CHECKPOINTS = f"{BASE}/_checkpoints"         # «пам'ять» stream: що вже оброблено + state вікон
SCHEMA_LOC = f"{BASE}/_schemas"              # службова інформація Auto Loader про схему
CACHE_DIR = f"{BASE}/_cache"                 # збережені результати повільних batch-запусків


def fq(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


T_BRONZE = fq("bronze_payments")
T_SILVER = fq("silver_payments")
T_QUAR = fq("quarantine_payments")
T_GOLD = fq("gold_hourly")
T_ALERTS = fq("alerts")
T_EDA = fq("eda_payments")            # копія сирих даних з типами, лише для EDA
T_BENCH = fq("benchmark_runs")        # вимірювання часу batch і streaming
T_BATCH_GOLD = fq("batch_gold_hourly")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONFIG і сценарій інциденту
# MAGIC
# MAGIC | Параметр | Що означає |
# MAGIC |---|---|
# MAGIC | `dims` | сегменти, для яких рахуємо схвалення (тут: канал × країна картки) |
# MAGIC | `window` | розмір часового вікна (1 година) |
# MAGIC | `watermark` | наскільки пізні дані ще приймаємо |
# MAGIC | `baseline_weeks` | з чим порівнюємо: та сама година того самого дня тижня за попередні N тижнів |
# MAGIC | `min_orders`, `min_baseline_orders` | не піднімаємо тривогу на дрібних сегментах (там просто шум) |
# MAGIC | `alert_drop_pp`, `alert_z` | алерт, якщо схвалення впало щонайменше на стільки п.п. **і** падіння статистично велике |
# MAGIC
# MAGIC `SCENARIO` описує **змодельований** інцидент: у четвер 14.09.2023 з 10:00 до 16:00 у каналі 6 частина успішних платежів
# MAGIC карток із США стає відмовами «Do not honor» (так виглядає, наприклад, збій у еквайра). Усе інше в даних справжнє.

# COMMAND ----------

# ---- settings: start ----
CONFIG = {
    "weight_col": "count_orders",       # один рядок = стільки замовлень
    "time_col": "event_time",
    "dims": ["channel_id", "card_country_grp"],
    "top_countries": ["USA", "GBR", "FRA", "ITA", "ESP", "AUS", "CAN", "CHL", "ZAF", "CHE"],   # решта -> OTHER
    "window": "1 hour",
    "watermark": "2 hours",
    "baseline_weeks": 4,
    "min_orders": 60,
    "min_baseline_orders": 250,
    "alert_drop_pp": 20,
    "alert_z": 4.5,
    "p1_drop_pp": 30,                   # падіння від 30 п.п. -> критичний алерт P1, інакше P2
}

# пороги для нічного batch-звіту (дані за цілу добу, тому інші числа)
BATCH_CONFIG = {"min_orders": 300, "min_baseline_orders": 1000, "alert_drop_pp": 10, "alert_z": 4.5}

SCENARIO = {
    "day": "2023-09-14",                # четвер
    "from_hour": 10, "to_hour": 16,     # інцидент триває 10:00-16:00
    "channel_id": "6", "card_country": "USA",
    "fail_share": 0.6,                  # 60% успішних платежів сегмента стають відмовами
    "error_code": "3.08", "error_message": "Do not honor",
    "seed": 7,
    "live_from_hour": 8,                # живий моніторинг у демо починаємо о 08:00
    "reaction_minutes": 30,             # скільки часу команді треба, щоб відреагувати після сигналу
    "batch_report_hour": 6,             # нічний batch-звіт готовий о 06:00 наступного дня
}

# середина кошика суми, щоб оцінити гроші (дані містять лише діапазони сум)
AMOUNT_MID = {"[0,10)": 5, "[10,20)": 15, "[20,40)": 30, "[40,60)": 50, "[60,100)": 80, "[100,150)": 125}
# ---- settings: end ----

# COMMAND ----------

# MAGIC %md
# MAGIC ## Допоміжні функції: «годинник» і надсилання даних у landing

# COMMAND ----------

# ---- helpers: start ----
RUNTIME = {}      # сюди зберігаємо останні метрики streaming, щоб показати їх на графіках


def n_fmt(x):
    """1234567 -> '1 234 567'"""
    return f"{int(round(x)):,}".replace(",", " ")


def file_hour(name):
    """payments_2023-09-14_10.csv -> 2023-09-14 10:00 (початок години, яку містить файл)."""
    return pd.Timestamp(f"{name[9:19]} {name[20:22]}:00")


def staged_files():
    if not os.path.exists(STAGING):
        raise FileNotFoundError("Немає папки staging. Спершу запустіть prepare_data().")
    return sorted(f for f in os.listdir(STAGING) if f.startswith("payments_") and f.endswith(".csv"))


def landing_files():
    return sorted(os.listdir(LANDING)) if os.path.exists(LANDING) else []


def clock():
    """Змодельований «поточний час»: кінець останньої години, яку вже надіслано в landing."""
    hours = [file_hour(f) for f in landing_files() if f.startswith("payments_")]
    return (max(hours) + pd.Timedelta(hours=1)) if hours else None


def release_until(until):
    """Надіслати в landing усі години, що закінчуються до `until`."""
    until = pd.Timestamp(until)
    already = set(landing_files())
    todo = [f for f in staged_files() if f not in already and file_hour(f) < until]
    os.makedirs(LANDING, exist_ok=True)
    for f in todo:
        shutil.copyfile(f"{STAGING}/{f}", f"{LANDING}/{f}")
    return todo


def release_next_hour():
    """Надіслати в landing наступну годину даних."""
    already = set(landing_files())
    todo = [f for f in staged_files() if f not in already]
    if not todo:
        return []
    os.makedirs(LANDING, exist_ok=True)
    shutil.copyfile(f"{STAGING}/{todo[0]}", f"{LANDING}/{todo[0]}")
    return [todo[0]]


def release_late(day="2023-04-03", hour=12):
    """Повторно надіслати СТАРУ годину під новою назвою: дані, що запізнилися на місяці."""
    src = f"{STAGING}/payments_{day}_{hour:02d}.csv"
    dst = f"{LANDING}/late_payments_{day}_{hour:02d}.csv"
    os.makedirs(LANDING, exist_ok=True)
    shutil.copyfile(src, dst)
    return dst.split("/")[-1]
# ---- helpers: end ----


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        try:
            spark.table(name).schema
            return True
        except Exception:
            return False


def safe_count(name):
    return spark.table(name).count() if table_exists(name) else 0


def reset_pipeline(drop_cache=False):
    """Почати streaming з нуля: видалити таблиці, checkpoints і landing. Архів staging лишається.
    drop_cache=True також видаляє EDA-таблицю, заміри batch і кеш нічного звіту."""
    for t in [T_BRONZE, T_SILVER, T_QUAR, T_GOLD, T_ALERTS]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    for p in [LANDING, CHECKPOINTS, SCHEMA_LOC]:
        shutil.rmtree(p, ignore_errors=True)
    if drop_cache:
        for t in [T_EDA, T_BENCH, T_BATCH_GOLD]:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
    RUNTIME.clear()
    print("🧹 Streaming-пайплайн скинуто: landing порожній, таблиці та checkpoints видалено."
          + (" Кеш теж видалено." if drop_cache else ""))


def progress_dicts(q):
    """Метрики streaming-запиту як список словників.
    На serverless `recentProgress` повертає об'єкти, а не словники, тому перетворюємо через JSON."""
    out = []
    for p in (q.recentProgress or []):
        if isinstance(p, dict):
            out.append(p)
            continue
        j = getattr(p, "json", None)
        try:
            j = j() if callable(j) else j
            out.append(json.loads(j))
            continue
        except Exception:
            pass
        try:
            out.append(json.loads(str(p)))
        except Exception:
            out.append({})
    return out


def stream_stats(q):
    ps = progress_dicts(q)
    rows = sum(int(p.get("numInputRows") or 0) for p in ps)
    dropped = sum(int(op.get("numRowsDroppedByWatermark") or 0)
                  for p in ps for op in (p.get("stateOperators") or []))
    last_ops = (ps[-1].get("stateOperators") or []) if ps else []
    return {
        "rows": rows,
        "batches": len(ps),
        "dropped_late": dropped,
        "state_rows": sum(int(op.get("numRowsTotal") or 0) for op in last_ops),
        "state_mb": sum(int(op.get("memoryUsedBytes") or 0) for op in last_ops) / 1e6,
    }


def record_bench(mode, as_of, files_read, rows_read, seconds, note=""):
    """Записати один замір (batch чи streaming) у таблицю benchmark_runs."""
    schema = "recorded_at TIMESTAMP, mode STRING, as_of TIMESTAMP, files_read BIGINT, rows_read BIGINT, seconds DOUBLE, note STRING"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {T_BENCH} ({schema})")
    row = [(datetime.datetime.now(), mode, pd.Timestamp(as_of).to_pydatetime(),
            int(files_read), int(rows_read), float(seconds), note)]
    spark.createDataFrame(row, schema=schema).write.mode("append").saveAsTable(T_BENCH)


def kpi_cards(items, accent="#4C78A8"):
    """Ряд карток із ключовими числами. items = [(підпис, значення, примітка), ...]"""
    cards = "".join(
        f"<div style='flex:1;min-width:150px;background:#ffffff;border:1px solid #e3e6ea;border-top:4px solid {accent};"
        f"border-radius:8px;padding:10px 14px;'>"
        f"<div style='font-size:12px;color:#5f6b7a;'>{label}</div>"
        f"<div style='font-size:22px;font-weight:700;color:#1a1a1a;margin-top:2px;'>{value}</div>"
        f"<div style='font-size:11px;color:#8a94a3;margin-top:2px;'>{note}</div></div>"
        for label, value, note in items)
    displayHTML(f"<div style='display:flex;gap:10px;flex-wrap:wrap;font-family:Arial,Helvetica,sans-serif;'>{cards}</div>")


def banner(text, color="#4C78A8", bg="#eef3f9"):
    displayHTML(f"<div style='font-family:Arial,Helvetica,sans-serif;background:{bg};border-left:5px solid {color};"
                f"padding:10px 14px;border-radius:6px;color:#1a1a1a;font-size:14px;margin:6px 0;'>{text}</div>")
