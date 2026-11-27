"""
NetHunt -> Google Ads: коригування конверсій "Prom Purchase (Ads tag)" для izovata.pro

Логіка:
  - Лід (Leads) з Order id перейшов у етап "Відхилено"   -> RETRACT (конверсія видаляється)
  - Угода (В2С угоди), пов'язана з лідом, має Бюджет і він змінився
                                                           -> RESTATE (цінність = Бюджет)
    Етап угоди не враховується ("Втрачена угода" не видаляється).
  - Після RETRACT замовлення більше не чіпаємо.

Результат пишеться в Google Таблицю, аркуш "upload" (формат шаблону коригувань Google Ads).
Google Ads забирає його запланованим завантаженням.
Стан (що вже відправлено) зберігається в аркуші "state", а не в репозиторії.

Змінні середовища (GitHub Secrets):
  NETHUNT_EMAIL, NETHUNT_API_KEY  - доступ до NetHunt
  GOOGLE_SA_JSON                  - JSON-ключ сервісного акаунта Google з доступом до таблиці
  SHEET_ID                        - ID Google Таблиці
"""
import json
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import gspread
import requests

LEADS_FOLDER_ID = "66222a15ab47d5633b86611e"  # Leads
DEALS_FOLDER_ID = "62a208e4331b7b685df23e2d"  # В2С угоди
SOURCE_MATCH = "izovat"
CONVERSION_NAME = "Prom Purchase (Ads tag)"
CURRENCY = "UAH"
TZ = ZoneInfo("Europe/Kiev")
MIN_AGE_HOURS = 24   # Google радить коригувати не раніше ніж через 24 год
MAX_AGE_DAYS = 54    # коригування можливі до 55 днів
LEAD_REJECTED = "Відхилено"
F_ORDER_ID = "Order id"
F_SOURCE = "Джерело"
F_STAGE = "Етап"
F_BUDGET = "Бюджет"
F_DEAL_LEAD_LINK = "Пов'язані записи"

API = "https://nethunt.com/api/v1/zapier"
STATE_HEADER = ["Order ID", "Lead ID", "Lead created", "Status", "Value", "Changed at", "Pending"]
UPLOAD_HEADER = ["Order ID", "Conversion Name", "Adjustment Time", "Adjustment Type",
                 "Adjusted Value", "Adjusted Value Currency"]


# ---------------- NetHunt ----------------

def nh_get(path, params=None):
    r = requests.get(f"{API}/{path}", params=params, timeout=60,
                     auth=(os.environ["NETHUNT_EMAIL"], os.environ["NETHUNT_API_KEY"]))
    if r.status_code >= 300:
        raise RuntimeError(f"NetHunt {r.status_code}: {r.text[:300]}")
    return r.json()


def rec_id(r):
    return r.get("recordId") or r.get("id")


def parse_dt(v):
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v / 1000, timezone.utc)
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


def fetch_updated(folder_id, since):
    """Записи, змінені після since (ISO), з пагінацією."""
    out, seen, cursor = [], set(), since
    for _ in range(50):
        page = nh_get(f"triggers/updated-record/{folder_id}", {"since": cursor, "limit": 100})
        fresh = [r for r in page if (rec_id(r), str(r.get("updatedAt"))) not in seen]
        for r in fresh:
            seen.add((rec_id(r), str(r.get("updatedAt"))))
            out.append(r)
        if len(page) < 100 or not fresh:
            break
        cursor = max_updated(page)
    return out


def find_record(folder_id, record_id):
    res = nh_get(f"searches/find-record/{folder_id}", {"recordId": record_id})
    if isinstance(res, list):
        return res[0] if res else None
    return res


def max_updated(rs):
    return max(parse_dt(r["updatedAt"]) for r in rs).isoformat()


# ---------------- утиліти ----------------

def first_str(v):
    if isinstance(v, list):
        return first_str(v[0]) if v else ""
    return "" if v is None else str(v).strip()


def to_num(v):
    s = re.sub(r"[^\d.,-]", "", str(v if v is not None else "")).replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def matches_source(fields):
    vals = fields.get(F_SOURCE) or []
    vals = vals if isinstance(vals, list) else [vals]
    return any(SOURCE_MATCH in str(x.get("name", x) if isinstance(x, dict) else x).lower() for x in vals)


def linked_ids(v):
    vals = v if isinstance(v, list) else ([v] if v else [])
    ids = [(x.get("recordId") or x.get("id")) if isinstance(x, dict) else str(x) for x in vals]
    return [i for i in ids if i and re.fullmatch(r"[0-9a-fA-F]{24}", i)]


# ---------------- Google Sheets ----------------

def open_sheet():
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

    return (ws("state", STATE_HEADER), ws("upload", UPLOAD_HEADER),
            ws("meta", ["key", "value"]), ws("log", ["Time", "Message"]))


def load_meta(w):
    return {r[0]: r[1] for r in w.get_all_values()[1:] if len(r) >= 2}


def save_meta(w, meta):
    w.batch_clear(["A2:B100"])
    if meta:
        w.update(range_name="A2", values=[[k, v] for k, v in meta.items()])


def load_state(w):
    st = {}
    for r in w.get_all_values()[1:]:
        if not r or not r[0]:
            continue
        r = r + [""] * (7 - len(r))
        st[r[0]] = {"orderId": r[0], "leadId": r[1], "created": r[2], "status": r[3],
                    "value": to_num(r[4]), "changedAt": r[5], "pending": r[6] == "TRUE"}
    return st


def save_state(w, st):
    rows = [[s["orderId"], s["leadId"], s["created"], s["status"], s["value"],
             s["changedAt"], "TRUE" if s["pending"] else "FALSE"] for s in st.values()]
    w.batch_clear(["A2:G10000"])
    if rows:
        w.update(range_name="A2", values=rows, value_input_option="RAW")


# ---------------- основна логіка ----------------

def new_state(order_id, lead_id, created):
    created_dt = parse_dt(created) if created else datetime.now(timezone.utc)
    return {"orderId": order_id, "leadId": lead_id, "created": created_dt.isoformat(),
            "status": "", "value": 0.0, "changedAt": "", "pending": False}


def main():
    w_state, w_upload, w_meta, w_log = open_sheet()
    meta = load_meta(w_meta)
    state = load_state(w_state)
    now = datetime.now(timezone.utc)
    default_since = (now - timedelta(days=MAX_AGE_DAYS)).isoformat()

    # 1. Ліди: нові Order id; "Відхилено" -> RETRACT
    leads = fetch_updated(LEADS_FOLDER_ID, meta.get("leadsSince", default_since))
    for r in leads:
        f = r.get("fields") or {}
        order_id = first_str(f.get(F_ORDER_ID))
        if not order_id or not matches_source(f):
            continue
        s = state.get(order_id) or new_state(order_id, rec_id(r), r.get("createdAt"))
        if first_str(f.get(F_STAGE)) == LEAD_REJECTED and s["status"] != "RETRACT":
            s.update(status="RETRACT", value=0.0, changedAt=now.isoformat(), pending=True)
        state[order_id] = s
    if leads:
        meta["leadsSince"] = max_updated(leads)

    # 2. Угоди: бюджет змінився -> RESTATE
    deals = fetch_updated(DEALS_FOLDER_ID, meta.get("dealsSince", default_since))
    for d in deals:
        f = d.get("fields") or {}
        if not matches_source(f):
            continue
        budget = to_num(f.get(F_BUDGET))
        for lead_id in linked_ids(f.get(F_DEAL_LEAD_LINK)):
            lead = find_record(LEADS_FOLDER_ID, lead_id)
            if not lead:
                continue
            order_id = first_str((lead.get("fields") or {}).get(F_ORDER_ID))
            if not order_id:
                continue
            s = state.get(order_id) or new_state(order_id, lead_id, lead.get("createdAt"))
            if budget > 0 and s["status"] != "RETRACT" and budget != s["value"]:
                s.update(status="RESTATE", value=budget, changedAt=now.isoformat(), pending=True)
            state[order_id] = s
    if deals:
        meta["dealsSince"] = max_updated(deals)

    # 3. Готові коригування -> upload
    adj_time = now.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S%z")
    rows = []
    for s in state.values():
        if not s["pending"]:
            continue
        age_h = (now - parse_dt(s["created"])).total_seconds() / 3600
        if age_h < MIN_AGE_HOURS:
            continue  # чекаємо наступного запуску
        if age_h > MAX_AGE_DAYS * 24:
            s["pending"] = False
            continue
        if s["status"] == "RETRACT":
            rows.append([s["orderId"], CONVERSION_NAME, adj_time, "RETRACT", "", ""])
        else:
            rows.append([s["orderId"], CONVERSION_NAME, adj_time, "RESTATE", f"{s['value']:.2f}", CURRENCY])
        s["pending"] = False

    # старі рядки (>55 днів) прибираємо
    existing = w_upload.get_all_values()[1:]
    cutoff = now - timedelta(days=55)
    keep = [r for r in existing if r and r[0] and _row_time(r) >= cutoff]
    all_rows = keep + rows
    w_upload.batch_clear(["A2:F100000"])
    if all_rows:
        w_upload.update(range_name="A2", values=all_rows, value_input_option="RAW")

    save_state(w_state, state)
    save_meta(w_meta, meta)
    msg = f"leads={len(leads)} deals={len(deals)} new_adjustments={len(rows)}"
    w_log.append_row([now.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S"), msg])
    print(msg)


def _row_time(r):
    try:
        return datetime.strptime(r[2], "%Y-%m-%d %H:%M:%S%z")
    except (ValueError, IndexError):
        return datetime.now(timezone.utc)


if __name__ == "__main__":
    main()
