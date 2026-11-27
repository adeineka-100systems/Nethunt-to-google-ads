"""
NetHunt -> Meta Conversions API:
  - успішні угоди (етап "Завершена угода") як DealWon / CallDealWon;
  - потенційні угоди (у "В2С угоди" з'явився бюджет) як PotentialPurchase / PotentialCall,
    одна подія на угоду (event_id = pot_<ID>), цінність = бюджет на момент виявлення,
    event_time = момент виявлення (для разового досилання з meta_potential_backfill.txt - час створення угоди);
    стан - аркуш meta_potential у Google Таблиці; угоди, створені понад 6 днів тому, не відправляються.

ТЗ: "передача успешных сделок из CRM в Meta Conversions API".
Запускається GitHub Actions кожні 15 хв (опитування API NetHunt замість вебхука, ТЗ розд. 4 п. 4).

Правила:
  - одна угода = одна подія: перевіряємо meta_sent_at + event_id = deal_<ID>;
  - event_time = дата закриття (поле "дата закриття угоди для аналітики", інакше "Дата укладання угоди",
    інакше момент виявлення); закриті понад 6 днів тому -> skipped;
  - Тип = Get Call / Вхідний / Вихідний або "Тип звернення" = дзвінок -> CallDealWon (phone_call), інше -> DealWon;
  - особисті дані нормалізуються і хешуються SHA-256, порожні поля не передаються;
  - мережеві помилки / 5xx / ліміт -> повтор через 1, 10, 60 хв, далі кожні 6 год, поки угоді < 6 днів;
  - 4xx -> без повтору, помилка в угоді, запуск завершується з помилкою (GitHub надсилає лист);
  - результат пишеться в угоду: meta_sent_at, meta_sent_value, meta_status;
  - лог спроб у Google Таблиці (аркуш meta_log), 90 днів, без телефонів/email/токена.

Тестовий режим: задайте секрет META_TEST_EVENT_CODE (код із Events Manager -> Тестові події).
  Події з тестовим кодом видно у вкладці "Тестові події", але Meta їх теж зараховує, тому угода
  позначається як відправлена (meta_status = "sent (test)") і повторно не піде.

Змінні середовища (GitHub Secrets):
  NETHUNT_EMAIL, NETHUNT_API_KEY, META_PIXEL_ID, META_ACCESS_TOKEN,
  GOOGLE_SA_JSON, SHEET_ID                       - обов'язкові
  META_TEST_EVENT_CODE                           - лише для тесту
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID           - необов'язково, сповіщення про помилки
  META_API_VERSION                               - необов'язково (за замовчуванням v26.0)
"""
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import gspread
import requests

DEALS_FOLDER_ID = "62a208e4331b7b685df23e2d"     # В2С угоди
CONTACTS_FOLDER_ID = "62a208e4331b7b685df23e2e"  # Контакти
WON_STAGE = "Завершена угода"
# Відправляємо ЛИШЕ угоди з сайту (поле "Тип" або "Тип звернення"). Усе інше ігнорується без запису статусу.
SITE_TYPES = {"кошик", "get call", "chat online"}   # -> DealWon
SITE_CONTACT_TYPES = {"кошик", "чат"}
CALL_TYPES = {"вхідний"}                             # вхідний дзвінок -> CallDealWon
SITE_SOURCE_TAGS = {"кошик", "корзина", "get call", "chat online"}  # теги "Джерело", якщо "Тип" порожній
BACKFILL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meta_backfill.txt")
# Потенційні: угода з сайту / вхідний дзвінок, у якій з'явився бюджет -> PotentialPurchase / PotentialCall
POT_SITE_EVENT, POT_CALL_EVENT = "PotentialPurchase", "PotentialCall"
POT_BACKFILL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meta_potential_backfill.txt")
POT_MAX_PER_RUN = 150      # решта піде наступним запуском
POT_KEEP_DAYS = 14
CURRENCY = "UAH"
MAX_AGE_DAYS = 6
RETRY_DELAYS_MIN = [1, 10, 60]   # далі кожні 6 год
RETRY_LATER_MIN = 360
LOG_KEEP_DAYS = 90
TZ = ZoneInfo("Europe/Kiev")
NH_API = "https://nethunt.com/api/v1/zapier"
API_VERSION = os.environ.get("META_API_VERSION", "v26.0")
TEST_CODE = os.environ.get("META_TEST_EVENT_CODE", "").strip()

# поля угоди (назви як у NetHunt)
F = {
    "stage": "Етап", "budget": "Бюджет", "type": "Тип", "contact_type": "Тип звернення",
    "close_analytics": "дата закриття угоди для аналітики", "close_date": "Дата укладання угоди",
    "contact": "Основний контакт", "source": "Джерело", "deal_phone": "Телефон в угоді", "number": "номер угоди",
    "fbc": "fbc", "fbp": "fbp", "ip": "fb_client_ip", "ua": "fb_user_agent", "url": "fb_source_url",
    "sent_at": "meta_sent_at", "sent_value": "meta_sent_value", "status": "meta_status",
}
C = {"fname": "Ім'я", "lname": "Прізвище", "phone": "Телефон", "email": ["Email", "Email адреса"],
     "city": "Місто (Селище) контакта"}


class HardError(Exception):
    """4xx від Meta: без повтору."""


# ---------------- NetHunt ----------------

def nh_auth():
    return (os.environ["NETHUNT_EMAIL"], os.environ["NETHUNT_API_KEY"])


def nh_get(path, params=None):
    r = requests.get(f"{NH_API}/{path}", params=params, auth=nh_auth(), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"NetHunt {r.status_code}: {r.text[:300]}")
    return r.json()


def nh_update(record_id, values):
    body = {"fieldActions": {k: {"overwrite": True, "add": v} for k, v in values.items()}}
    r = requests.post(f"{NH_API}/actions/update-record/{record_id}", params={"overwrite": "true"},
                      json=body, auth=nh_auth(), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"NetHunt update {r.status_code}: {r.text[:300]}")


def rec_id(r):
    return r.get("recordId") or r.get("id")


def fetch_updated(folder_id, since):
    out, seen, cursor = [], set(), since
    for _ in range(50):
        page = nh_get(f"triggers/updated-record/{folder_id}", {"since": cursor, "limit": 100})
        fresh = [r for r in page if (rec_id(r), str(r.get("updatedAt"))) not in seen]
        for r in fresh:
            seen.add((rec_id(r), str(r.get("updatedAt"))))
            out.append(r)
        if len(page) < 100 or not fresh:
            break
        cursor = max(parse_dt(r["updatedAt"]) for r in page).isoformat()
    return out


def find_record(folder_id, record_id):
    res = nh_get(f"searches/find-record/{folder_id}", {"recordId": record_id})
    if isinstance(res, list):
        return res[0] if res else None
    return res


# ---------------- утиліти ----------------

def parse_dt(v):
    """epoch-ms, ISO-дата або ISO-час -> aware datetime (UTC)."""
    if v in (None, "", []):
        return None
    if isinstance(v, list):
        return parse_dt(v[0])
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, timezone.utc)
    s = str(v).strip()
    if re.fullmatch(r"\d{10,13}", s):
        return parse_dt(int(s))
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.fromisoformat(s).replace(tzinfo=TZ).astimezone(timezone.utc)
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def values(v):
    """Значення поля -> список рядків."""
    if v in (None, ""):
        return []
    items = v if isinstance(v, list) else [v]
    out = []
    for x in items:
        if isinstance(x, dict):
            x = x.get("value") or x.get("name") or x.get("email") or x.get("phone") or x.get("recordId") or x.get("id")
        if x not in (None, ""):
            out.append(str(x).strip())
    return out


def first(v):
    vals = values(v)
    return vals[0] if vals else ""


def to_num(v):
    s = re.sub(r"[^\d.,-]", "", first(v)).replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return 0.0


def sha(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def norm_phone(p):
    d = re.sub(r"\D", "", p or "")
    if len(d) == 10 and d.startswith("0"):
        d = "38" + d
    elif len(d) == 9:
        d = "380" + d
    elif len(d) == 11 and d.startswith("80"):
        d = "3" + d
    return d if len(d) >= 11 else ""


def norm_name(s):
    return re.sub(r"[\W\d_]+", "", (s or "").lower())


def norm_city(s):
    return re.sub(r"[\s\W\d_]+", "", (s or "").lower())


# ---------------- подія ----------------

def deal_kind(f):
    """'site' (DealWon), 'call' (CallDealWon) або None (не відправляємо).
    Спершу поле "Тип"; якщо порожнє - теги "Джерело"; далі "Тип звернення"."""
    t = first(f.get(F["type"])).lower()
    if t:
        return "site" if t in SITE_TYPES else "call" if t in CALL_TYPES else None
    tags = {x.lower() for x in values(f.get(F["source"]))}
    if tags & SITE_SOURCE_TAGS:
        return "site"
    if tags & CALL_TYPES:
        return "call"
    if first(f.get(F["contact_type"])).lower() in SITE_CONTACT_TYPES:
        return "site"
    return None


def close_time(f, now):
    """(event_time, close_dt) або (None, причина пропуску)."""
    close = parse_dt(f.get(F["close_analytics"]))
    if close is None:
        cd = parse_dt(f.get(F["close_date"]))
        close = cd if cd is not None and cd <= now else now  # "Дата укладання" часто стоїть у майбутньому
    if close > now:
        close = now
    if now - close > timedelta(days=MAX_AGE_DAYS):
        return None, "skipped: older than 7 days"
    # дата без часу -> кінець того дня, але не пізніше "зараз"
    if close.astimezone(TZ).time() == datetime.min.time():
        close = min(now, close + timedelta(hours=23, minutes=59))
    return close, None


def build_user_data(deal, contact):
    f = deal.get("fields") or {}
    cf = (contact or {}).get("fields") or {}
    ud = {}
    phones = {norm_phone(p) for p in values(cf.get(C["phone"])) + values(f.get(F["deal_phone"]))} - {""}
    if phones:
        ud["ph"] = [sha(p) for p in sorted(phones)]
    emails = {e.lower() for key in C["email"] for e in values(cf.get(key)) if "@" in e}
    if emails:
        ud["em"] = [sha(e) for e in sorted(emails)]
    fn, ln = norm_name(first(cf.get(C["fname"]))), norm_name(first(cf.get(C["lname"])))
    if fn:
        ud["fn"] = [sha(fn)]
    if ln:
        ud["ln"] = [sha(ln)]
    ct = norm_city(first(cf.get(C["city"])))
    if ct:
        ud["ct"] = [sha(ct)]
    ud["country"] = [sha("ua")]
    if contact:
        ud["external_id"] = [sha(rec_id(contact))]
    for key, meta_key in (("fbc", "fbc"), ("fbp", "fbp"), ("ip", "client_ip_address"), ("ua", "client_user_agent")):
        v = first(f.get(F[key]))
        if v:
            ud[meta_key] = v
    return ud


def make_event(deal, contact, name, event_id, etime, value, is_call):
    f = deal.get("fields") or {}
    event = {
        "event_name": name,
        "event_time": int(etime.timestamp()),
        "event_id": event_id,
        "action_source": "phone_call" if is_call else "system_generated",
        "user_data": build_user_data(deal, contact),
        "custom_data": {"value": value, "currency": CURRENCY, "order_id": rec_id(deal)},
    }
    url = first(f.get(F["url"]))
    if url and not is_call:
        event["event_source_url"] = url
    return event


def build_event(deal, contact, now):
    f = deal.get("fields") or {}
    etime, skip = close_time(f, now)
    if skip:
        return None, skip
    value = to_num(f.get(F["budget"]))
    if value <= 0:
        return None, "waiting: no budget"  # не блокує: відправиться, коли з'явиться бюджет
    is_call = deal_kind(f) == "call"
    return make_event(deal, contact, "CallDealWon" if is_call else "DealWon", f"deal_{rec_id(deal)}",
                      etime, value, is_call), None


def send_to_meta(event):
    payload = {"data": [event]}
    if TEST_CODE:
        payload["test_event_code"] = TEST_CODE
    url = f"https://graph.facebook.com/{API_VERSION}/{os.environ['META_PIXEL_ID']}/events"
    try:
        r = requests.post(url, params={"access_token": os.environ["META_ACCESS_TOKEN"]}, json=payload, timeout=60)
    except requests.RequestException as e:
        return None, f"network: {type(e).__name__}"
    try:
        body = r.json()
    except ValueError:
        body = {"raw": r.text[:300]}
    return r.status_code, body


# ---------------- Google Sheets (стан, черга повторів, лог) ----------------

def open_sheets():
    gc = gspread.service_account_from_dict(json.loads(os.environ["GOOGLE_SA_JSON"]))
    ss = gc.open_by_key(os.environ["SHEET_ID"])

    def ws(name, header):
        try:
            w = ss.worksheet(name)
        except gspread.WorksheetNotFound:
            w = ss.add_worksheet(name, rows=1000, cols=len(header))
        if not w.row_values(1):
            w.update(range_name="A1", values=[header])
        return w

    return (ws("meta_state", ["key", "value"]),
            ws("meta_queue", ["deal_id", "attempts", "next_try", "last_error"]),
            ws("meta_log", ["time", "mode", "deal_id", "deal_no", "event", "value", "user_data_fields",
                            "http", "result", "fbtrace_id"]),
            ws("meta_potential", ["deal_id", "deal_no", "event", "value", "event_time", "sent_at", "status"]))


def read_kv(w):
    return {r[0]: r[1] for r in w.get_all_values()[1:] if len(r) >= 2 and r[0]}


def write_rows(w, rows, width):
    w.batch_clear([f"A2:{chr(64 + width)}100000"])
    if rows:
        w.update(range_name="A2", values=rows, value_input_option="RAW")


def notify(text):
    tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    print("NOTIFY:", text)
    if tok and chat:
        try:
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": text[:3500]}, timeout=20)
        except requests.RequestException:
            pass


# ---------------- потенційні ----------------

def process_potential(updated, w_pot, now, mode, log_rows):
    """Угоди з бюджетом -> PotentialPurchase / PotentialCall, по одній події на угоду.
    Стан (що вже відправлено) - аркуш meta_potential. Повертає список критичних помилок."""
    rows = w_pot.get_all_values()[1:]
    pot = {r[0]: r for r in rows if r and r[0]}
    done = {k for k, r in pot.items() if len(r) > 6 and not r[6].startswith("retry")}

    # кандидати: свіжі зміни (event_time = зараз) + разове досилання з файлу (event_time = створення угоди)
    cands = [(d, False) for d in updated if rec_id(d) not in done]
    seen = {rec_id(d) for d, _ in cands}
    if os.path.exists(POT_BACKFILL_FILE):
        for line in open(POT_BACKFILL_FILE, encoding="utf-8"):
            deal_id = line.split("#")[0].strip()
            if deal_id and deal_id not in done and deal_id not in seen:
                cands.append((deal_id, True))
                seen.add(deal_id)

    contacts, errors, sent_count = {}, [], 0
    for item, from_file in cands:
        if sent_count >= POT_MAX_PER_RUN:
            break
        deal = find_record(DEALS_FOLDER_ID, item) if from_file else item
        if not deal:
            continue
        deal_id = rec_id(deal)
        f = deal.get("fields") or {}
        kind = deal_kind(f)
        value = to_num(f.get(F["budget"]))
        created = parse_dt(deal.get("createdAt"))
        if kind is None or value <= 0 or created is None:
            continue
        if now - created > timedelta(days=MAX_AGE_DAYS):
            # стара угода, якій просто щось змінили: бюджет з'явився давно, Meta таку подію вже не прийме
            continue
        prev = pot.get(deal_id)
        if prev and len(prev) > 4 and prev[4]:
            etime = parse_dt(prev[4])          # повтор після помилки - той самий час події
        else:
            etime = min(created, now) if from_file else now
        if etime is None or now - etime > timedelta(days=MAX_AGE_DAYS):
            continue
        is_call = kind == "call"
        name = POT_CALL_EVENT if is_call else POT_SITE_EVENT
        cid = first(f.get(F["contact"]))
        if cid and cid not in contacts:
            contacts[cid] = find_record(CONTACTS_FOLDER_ID, cid)
        event = make_event(deal, contacts.get(cid), name, f"pot_{deal_id}", etime, value, is_call)
        code, body = send_to_meta(event)
        sent_count += 1
        stamp = now.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
        deal_no = first(f.get(F["number"]))
        fbtrace = ""
        if isinstance(body, dict):
            fbtrace = body.get("fbtrace_id") or (body.get("error") or {}).get("fbtrace_id", "")
        fields_used = ",".join(sorted(event["user_data"].keys()))
        etime_s = etime.isoformat(timespec="seconds")
        if code == 200 and isinstance(body, dict) and body.get("events_received") == 1:
            status = "sent (test)" if mode == "test" else "sent"
            pot[deal_id] = [deal_id, deal_no, name, value, etime_s, stamp, status]
            log_rows.append([stamp, mode, deal_id, deal_no, name, value, fields_used, code, "sent", fbtrace])
        else:
            err_obj = body.get("error") if isinstance(body, dict) else None
            err = (err_obj.get("message") if isinstance(err_obj, dict) else None) or str(body)[:200]
            retryable = code is None or code >= 500 or code == 429 or (
                isinstance(err_obj, dict) and err_obj.get("code") in (4, 17, 32, 613))
            pot[deal_id] = [deal_id, deal_no, name, value, etime_s, stamp,
                            ("retry: " if retryable else "error: ") + err[:200]]
            log_rows.append([stamp, mode, deal_id, deal_no, name, value, fields_used, code or "", f"error: {err}", fbtrace])
            if not retryable:
                auth = isinstance(err_obj, dict) and err_obj.get("code") in (102, 190)
                msg = (f"META CAPI: ТОКЕН ВІДХИЛЕНО, усі відправки зупинено. {err}" if auth
                       else f"META CAPI: помилка {name} для угоди {deal_no or deal_id}: {err} (fbtrace {fbtrace})")
                notify(msg)
                errors.append(msg)
                if auth:
                    break
        time.sleep(0.1)

    cutoff = now - timedelta(days=POT_KEEP_DAYS)
    keep = [r for r in pot.values() if not parse_dt(r[4] if len(r) > 4 else None) or parse_dt(r[4]) >= cutoff]
    write_rows(w_pot, keep, 7)
    print(f"potential: candidates={len(cands)} sent_attempts={sent_count}")
    return errors


# ---------------- основний цикл ----------------

def main():
    if not os.environ.get("META_PIXEL_ID") or not os.environ.get("META_ACCESS_TOKEN"):
        print("META_PIXEL_ID / META_ACCESS_TOKEN ще не задані в Secrets - пропускаю запуск")
        return
    now = datetime.now(timezone.utc)
    mode = "test" if TEST_CODE else "prod"
    w_state, w_queue, w_log, w_pot = open_sheets()
    state = read_kv(w_state)
    queue = {r[0]: {"attempts": int(r[1] or 0), "next_try": r[2], "last_error": r[3] if len(r) > 3 else ""}
             for r in w_queue.get_all_values()[1:] if r and r[0]}
    since_key = "since"
    since = state.get(since_key) or (now - timedelta(days=MAX_AGE_DAYS)).isoformat()

    updated = fetch_updated(DEALS_FOLDER_ID, since)
    deals = {rec_id(d): d for d in updated}
    if len(updated) >= 100:
        # API NetHunt віддає не більше 100 найновіших змін: частина могла не потрапити
        state["warning"] = f"{now.astimezone(TZ):%Y-%m-%d %H:%M}: отримано 100 змін за запуск, можливі пропуски"
    # одноразове досилання: ID угод із meta_backfill.txt (уже відправлені пропускаються)
    if os.path.exists(BACKFILL_FILE):
        for line in open(BACKFILL_FILE, encoding="utf-8"):
            deal_id = line.split("#")[0].strip()
            if deal_id and deal_id not in deals:
                d = find_record(DEALS_FOLDER_ID, deal_id)
                if d and not first((d.get("fields") or {}).get(F["sent_at"])):
                    deals[deal_id] = d
    # повтори, яким настав час
    for deal_id, q in queue.items():
        if deal_id not in deals and parse_dt(q["next_try"]) and parse_dt(q["next_try"]) <= now:
            d = find_record(DEALS_FOLDER_ID, deal_id)
            if d:
                deals[deal_id] = d

    log_rows, hard_errors = [], []
    for deal_id, deal in deals.items():
        f = deal.get("fields") or {}
        if first(f.get(F["stage"])) != WON_STAGE or deal_kind(f) is None:
            queue.pop(deal_id, None)
            continue
        status = first(f.get(F["status"]))
        q = queue.get(deal_id)
        if (first(f.get(F["sent_at"])) or status.startswith("skipped") or (mode == "test" and status == "test: sent")
                or (status.startswith("error") and not q)):
            # error без запису в черзі = остаточна помилка 4xx; щоб відправити знову, очистіть meta_status
            queue.pop(deal_id, None)
            continue
        if q and parse_dt(q["next_try"]) and parse_dt(q["next_try"]) > now:
            continue

        contact_id = first(f.get(F["contact"]))
        contact = find_record(CONTACTS_FOLDER_ID, contact_id) if contact_id else None
        event, skip = build_event(deal, contact, now)
        deal_no = first(f.get(F["number"]))
        stamp = now.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")

        if skip:
            nh_update(deal_id, {F["status"]: skip})
            queue.pop(deal_id, None)
            log_rows.append([stamp, mode, deal_id, deal_no, "", "", "", "", skip, ""])
            continue

        code, body = send_to_meta(event)
        fields_used = ",".join(sorted(event["user_data"].keys()))
        fbtrace = ""
        if isinstance(body, dict):
            fbtrace = body.get("fbtrace_id") or (body.get("error") or {}).get("fbtrace_id", "")
        value = event["custom_data"]["value"]

        if code == 200 and isinstance(body, dict) and body.get("events_received") == 1:
            # події з test_event_code Meta теж зараховує, тому і в тесті позначаємо угоду як відправлену
            nh_update(deal_id, {F["sent_at"]: now.astimezone(TZ).isoformat(timespec="seconds"),
                                F["sent_value"]: value, F["status"]: "sent (test)" if mode == "test" else "sent"})
            queue.pop(deal_id, None)
            log_rows.append([stamp, mode, deal_id, deal_no, event["event_name"], value, fields_used, code, "sent", fbtrace])
            continue

        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            err = body["error"].get("message") or str(body["error"])[:200]
        else:
            err = str(body)[:200]
        retryable = code is None or code >= 500 or code == 429 or (
            isinstance(body, dict) and isinstance(body.get("error"), dict) and body["error"].get("code") in (4, 17, 32, 613))
        log_rows.append([stamp, mode, deal_id, deal_no, event["event_name"], value, fields_used, code or "", f"error: {err}", fbtrace])
        nh_update(deal_id, {F["status"]: f"error: {err}"[:250]})

        if retryable:
            attempts = (q["attempts"] if q else 0) + 1
            delay = RETRY_DELAYS_MIN[attempts - 1] if attempts <= len(RETRY_DELAYS_MIN) else RETRY_LATER_MIN
            queue[deal_id] = {"attempts": attempts, "next_try": (now + timedelta(minutes=delay)).isoformat(), "last_error": err}
        else:
            queue.pop(deal_id, None)
            auth = isinstance(body, dict) and isinstance(body.get("error"), dict) and body["error"].get("code") in (102, 190)
            msg = (f"META CAPI: ТОКЕН ВІДХИЛЕНО, усі відправки зупинено. {err}" if auth
                   else f"META CAPI: помилка відправки угоди {deal_no or deal_id}: {err} (fbtrace {fbtrace})")
            notify(msg)
            hard_errors.append(msg)
            if auth:
                break
        time.sleep(0.2)

    # ---------- потенційні угоди (з'явився бюджет) ----------
    if not any("ТОКЕН ВІДХИЛЕНО" in m for m in hard_errors):
        pot_errors = process_potential(updated, w_pot, now, mode, log_rows)
        hard_errors += pot_errors

    # (угоди старші 6 днів самі вийдуть з черги: build_event поверне skipped)

    # збереження стану
    if deals:
        fetched = [parse_dt(d.get("updatedAt")) for d in deals.values() if parse_dt(d.get("updatedAt"))]
        if fetched:
            state[since_key] = max(fetched).isoformat()
    write_rows(w_state, [[k, v] for k, v in state.items()], 2)
    write_rows(w_queue, [[k, v["attempts"], v["next_try"], v["last_error"]] for k, v in queue.items()], 4)
    existing = w_log.get_all_values()[1:]
    cutoff = (now - timedelta(days=LOG_KEEP_DAYS)).astimezone(TZ).strftime("%Y-%m-%d")
    write_rows(w_log, [r for r in existing if r and r[0][:10] >= cutoff] + log_rows, 10)

    print(f"mode={mode} deals_checked={len(deals)} logged={len(log_rows)} queue={len(queue)}")
    if hard_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
