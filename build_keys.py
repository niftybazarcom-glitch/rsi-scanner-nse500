"""
KEYS tab: Upstox instrument keys (stock naam -> NSE_EQ|ISIN), roz subah refresh.
STOPLOSS ALERT (Apps Script) isi tab se live price ki key padhta hai.
Kisi stock ka ISIN badal jaye to roz refresh hone se key apne aap theek ho jati hai.
"""
import gzip
import json
import os
from datetime import datetime, timedelta, timezone

import requests

IST = timezone(timedelta(hours=5, minutes=30))
URLS = [
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz",
]
TAB_NAME = "KEYS"
MIN_ROWS = 300  # NSE equities hazaron mein hain; isse kam mile to file ka format badla hoga

SHEET_ID = os.environ.get("SHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")


def download():
    for url in URLS:
        try:
            r = requests.get(url, timeout=180)
            if r.status_code != 200:
                print("HTTP %s: %s" % (r.status_code, url))
                continue
            data = json.loads(gzip.decompress(r.content))
            if isinstance(data, list) and data:
                print("Instrument file mili: %d records" % len(data))
                return data
        except Exception as e:  # noqa: BLE001
            print("Download/parse fail (%s): %s" % (type(e).__name__, url))
    raise SystemExit("Upstox instruments file download nahi ho payi.")


def build_rows(data):
    best = {}
    for it in data:
        if it.get("segment") != "NSE_EQ":
            continue
        symbol = (it.get("trading_symbol") or "").strip()
        key = it.get("instrument_key")
        if not symbol or not key:
            continue
        typ = it.get("instrument_type") or ""
        cur = best.get(symbol)
        if cur is None or (cur[2] != "EQ" and typ == "EQ"):  # EQ series ko BE/SM pe tarjeeh
            best[symbol] = (key, it.get("name") or "", typ)
    return [[s, v[0], v[1], v[2]] for s, v in sorted(best.items())]


def open_book():
    import gspread

    if not SHEET_ID or not GOOGLE_CREDENTIALS:
        raise SystemExit("SHEET_ID ya GOOGLE_CREDENTIALS secret set nahi hai.")
    client = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS))
    return client.open_by_key(SHEET_ID)


def get_or_create_tab(book, rows_needed):
    from gspread.exceptions import WorksheetNotFound

    try:
        return book.worksheet(TAB_NAME)
    except WorksheetNotFound:
        return book.add_worksheet(title=TAB_NAME, rows=rows_needed + 10, cols=6)


def main():
    rows = build_rows(download())
    if len(rows) < MIN_ROWS:
        raise SystemExit("Bahut kam NSE_EQ instruments mile (%d). File ka format badla ho sakta hai." % len(rows))
    stamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M IST")
    header = ["Symbol", "Instrument Key", "Name", "Type", "Updated " + stamp]

    ws = get_or_create_tab(open_book(), len(rows) + 1)
    ws.clear()
    ws.update(range_name="A1", values=[header] + rows, value_input_option="RAW")
    print("KEYS tab refresh: %d stocks." % len(rows))


if __name__ == "__main__":
    main()
