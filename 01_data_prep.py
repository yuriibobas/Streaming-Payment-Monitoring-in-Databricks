# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Підготовка «живого потоку»
# MAGIC
# MAGIC Ноутбук **не запускається окремо**, функцію `prepare_data()` викликає `06_demo`.
# MAGIC
# MAGIC У CSV є лише **дата** (`2023-04-01 00:00:00`), тому з нього не можна просто «програти» потік. `prepare_data()`:
# MAGIC
# MAGIC 1. додає кожному рядку **синтетичний час доби** (`event_time`): удень і ввечері подій більше, вночі менше;
# MAGIC 2. додає **змодельований інцидент** зі `SCENARIO` (14.09.2023, 10:00-16:00, канал 6, картки США);
# MAGIC 3. нарізає дані на **погодинні файли** у `staging/` (близько 4 300 файлів).
# MAGIC
# MAGIC Оригінальний файл не змінюється. Неіснуючі дати (наприклад, `2023-09-31`) **залишаються як є**:
# MAGIC пайплайн має їх виловити сам і відправити в карантин.

# COMMAND ----------

# ---- prepare: start ----
# ймовірність потрапити в кожну годину доби (0 ... 23)
HOUR_WEIGHTS = [2, 1.5, 1, 1, 1, 1.5, 2.5, 4, 5.5, 6.5, 7, 7, 6.5, 6.5, 6, 6, 6, 6.5, 7, 7.5, 7.5, 6.5, 5, 3.5]


def prepare_data(force=False):
    marker = f"{STAGING}/_READY.json"
    if os.path.exists(marker) and not force:
        info = json.load(open(marker))
        print(f"✅ Дані вже підготовлені: {n_fmt(info['files'])} погодинних файлів, {n_fmt(info['rows'])} рядків. "
              "Щоб перебудувати, викличте prepare_data(force=True).")
        return info
    if not os.path.exists(RAW_CSV):
        raise FileNotFoundError(f"Не знайдено {RAW_CSV}. Завантажте BillingOps-data.csv у Volume "
                                "(Catalog -> workspace -> payments_streaming -> Volumes -> data -> Upload to this volume).")
    t0 = time.time()
    df = pd.read_csv(RAW_CSV, sep=";", dtype=str, keep_default_na=False, index_col=False)
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]       # кожен рядок закінчується ';' -> порожня колонка
    n = len(df)

    # 1) синтетичний час доби (однаковий seed -> однаковий результат щоразу)
    rng = np.random.default_rng(42)
    p = np.array(HOUR_WEIGHTS, dtype=float)
    p = p / p.sum()
    hours = rng.choice(24, size=n, p=p)

    def two(a):
        return pd.Series(a, index=df.index).astype(str).str.zfill(2)

    # дату беремо як текст: неіснуюча дата (2023-09-31) так і лишиться неіснуючою
    df["event_time"] = (df["created_day"].str[:10] + " " + two(hours) + ":"
                        + two(rng.integers(0, 60, n)) + ":" + two(rng.integers(0, 60, n)))

    # 2) до якого дня-файлу належить рядок; неіснуючі дати йдуть в останній реальний день місяця
    y = df["created_day"].str[0:4].astype(int)
    m = df["created_day"].str[5:7].astype(int)
    d = df["created_day"].str[8:10].astype(int)
    month_start = pd.to_datetime(pd.DataFrame({"year": y, "month": m, "day": 1}))
    df["_day"] = (month_start + pd.to_timedelta(np.minimum(d, month_start.dt.days_in_month) - 1, unit="D")).dt.strftime("%Y-%m-%d")
    df["_hour"] = hours

    # 3) змодельований інцидент
    s = SCENARIO
    rng2 = np.random.default_rng(s["seed"])
    hit = ((df["_day"] == s["day"]) & (df["_hour"] >= s["from_hour"]) & (df["_hour"] < s["to_hour"])
           & (df["channel_id"] == s["channel_id"]) & (df["card_country"] == s["card_country"])
           & (df["status"] == "success"))
    flip = hit & pd.Series(rng2.random(n) < s["fail_share"], index=df.index)
    df.loc[flip, "status"] = "fail"
    df.loc[flip, "error_code"] = s["error_code"]
    df.loc[flip, "error_message"] = s["error_message"]

    # 4) погодинні файли
    shutil.rmtree(STAGING, ignore_errors=True)
    os.makedirs(STAGING, exist_ok=True)
    cols = [c for c in df.columns if not c.startswith("_")]
    files = 0
    for (day, hour), g in df.groupby(["_day", "_hour"], sort=True):
        g[cols].sort_values("event_time").to_csv(f"{STAGING}/payments_{day}_{int(hour):02d}.csv", sep=";", index=False)
        files += 1

    info = {"rows": n, "files": files, "first_day": df["_day"].min(), "last_day": df["_day"].max(),
            "incident_rows_flipped": int(flip.sum()),
            "incident_orders_flipped": int(df.loc[flip, "count_orders"].astype(int).sum()),
            "seconds": round(time.time() - t0, 1)}
    with open(marker, "w") as f:
        json.dump(info, f)
    print(f"✅ Готово за {info['seconds']} с: {n_fmt(files)} погодинних файлів, {n_fmt(n)} рядків, "
          f"{info['first_day']} … {info['last_day']}.")
    print(f"   Змодельований інцидент: {n_fmt(info['incident_rows_flipped'])} рядків success -> fail "
          f"({s['day']}, {s['from_hour']}:00-{s['to_hour']}:00, канал {s['channel_id']}, картки {s['card_country']}).")
    return info
# ---- prepare: end ----


def peek_staging():
    """Подивитися на один погодинний файл: що саме отримає stream."""
    f = staged_files()[0]
    print("Файл:", f)
    display(spark.read.option("header", True).option("sep", ";").csv(f"{STAGING}/{f}").limit(10))
