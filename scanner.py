"""
RSI SCANNER: Nifty 500, Upstox data -> Google Sheet tab "RSI SCANNER"

- 1 HOUR RSI: har 30 minute mein   (MODE=hourly)
- 1 DAY  RSI: har 1 ghante mein    (MODE=daily)
- RSI(14) 80 se upar ya 20 se neeche wale stocks sheet mein judte hain
- Ek stock din mein ek hi baar aata hai (kisi bhi timeframe mein)
- Chalti hui candle ka RSI use hota hai
"""
import csv
import gzip
import io
import json
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests

IST = timezone(timedelta(hours=5, minutes=30))
BASE = "https://api.upstox.com/v3/historical-candle"

TOKEN = os.environ.get("UPSTOX_TOKEN", "")
SHEET_ID = os.environ.get("SHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
MODE = os.environ.get("MODE", "hourly")  # hourly | daily | both
FORCE = os.environ.get("FORCE", "").strip().lower() in ("1", "true", "yes")

TAB_NAME = "RSI SCANNER"
HEADER = ["Date", "Time", "Stock", "Timeframe", "RSI", "Price", "Signal"]

RSI_PERIOD = 14
UPPER = 80
LOWER = 20

HIST_HOURS_DAYS = 45    # purani 1 HOUR candles (1 quarter ki limit ke andar)
HIST_DAYS_DAYS = 200    # purani 1 DAY candles
KEEP_HOURS = 150        # cache mein kitni candles rakhni hain
KEEP_DAYS = 100
CACHE_FILE = "hist_cache.json.gz"
MIN_GAP = 0.12          # do API calls ke beech minimum seconds (Upstox limit se bahut neeche)

NIFTY500_URLS = [
    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
]

_last_call = 0.0


# ---------------------------------------------------------------- Upstox API
def api_get(path):
    """Candles list return karta hai. Error par None, data na ho to []."""
    global _last_call
    url = BASE + "/" + path
    headers = {"Accept": "application/json", "Authorization": "Bearer " + TOKEN}
    for attempt in range(4):
        wait = MIN_GAP - (time.time() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.time()
        try:
            r = requests.get(url, headers=headers, timeout=20)
        except requests.RequestException:
            time.sleep(2)
            continue
        if r.status_code == 200:
            try:
                return r.json().get("data", {}).get("candles", []) or []
            except ValueError:
                return None
        if r.status_code == 429:
            time.sleep(10 * (attempt + 1))
            continue
        if r.status_code in (401, 403):
            raise SystemExit("Upstox ne token reject kiya (HTTP %s). UPSTOX_TOKEN check karo." % r.status_code)
        return None
    return None


def key_enc(instrument):
    return quote(instrument, safe="")


def fetch_hist(instrument, unit, days_back, today):
    frm = (today - timedelta(days=days_back)).isoformat()
    return api_get("%s/%s/1/%s/%s" % (key_enc(instrument), unit, today.isoformat(), frm))


def fetch_intraday(instrument, unit):
    return api_get("intraday/%s/%s/1" % (key_enc(instrument), unit))


# ---------------------------------------------------------------- candles / RSI
def candle_key(ts, unit):
    ts = str(ts)
    return ts[:10] if unit == "days" else ts


def to_pairs(candles, unit):
    """[[ts, o, h, l, c, ...]] -> {key: close}"""
    out = {}
    for c in candles or []:
        try:
            out[candle_key(c[0], unit)] = float(c[4])
        except (IndexError, TypeError, ValueError):
            continue
    return out


def last_pairs(candles, unit, keep):
    items = sorted(to_pairs(candles, unit).items())
    return [[k, v] for k, v in items[-keep:]]


def merged_series(hist_pairs, intraday_candles, unit):
    d = {k: v for k, v in hist_pairs}
    d.update(to_pairs(intraday_candles, unit))  # aaj ki candle purani ko override karti hai
    return sorted(d.items())


def rsi(closes, n=RSI_PERIOD):
    """Wilder RSI."""
    if len(closes) < n + 1:
        return None
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    avg_gain, avg_loss = gains / n, losses / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (n - 1) + max(d, 0)) / n
        avg_loss = (avg_loss * (n - 1) + max(-d, 0)) / n
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


# ---------------------------------------------------------------- stock list
def load_nifty500():
    text = None
    if os.path.exists("nifty500.csv"):
        with open("nifty500.csv", encoding="utf-8-sig") as f:
            text = f.read()
    else:
        ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}
        for url in NIFTY500_URLS:
            try:
                r = requests.get(url, headers=ua, timeout=30)
                if r.status_code == 200 and "ISIN" in r.text:
                    text = r.text
                    break
            except requests.RequestException:
                continue
    if not text:
        raise SystemExit(
            "Nifty 500 list nahi mili. NSE/niftyindices se 'ind_nifty500list.csv' download karke "
            "repo mein 'nifty500.csv' naam se upload karo."
        )
    stocks = []
    for row in csv.DictReader(io.StringIO(text)):
        row = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
        symbol, isin = row.get("Symbol"), row.get("ISIN Code")
        if symbol and isin:
            stocks.append((symbol, "NSE_EQ|" + isin))
    if not stocks:
        raise SystemExit("Nifty 500 file padhi nahi gayi (Symbol / ISIN Code columns chahiye).")
    return stocks


# ---------------------------------------------------------------- history cache
def load_cache(today_s):
    try:
        with gzip.open(CACHE_FILE, "rt", encoding="utf-8") as f:
            c = json.load(f)
        if c.get("date") == today_s:
            return c
    except Exception:
        pass
    return {"date": today_s, "data": {}}


def save_cache(cache):
    with gzip.open(CACHE_FILE, "wt", encoding="utf-8") as f:
        json.dump(cache, f)


def get_hist(cache, instrument, unit, today_d):
    entry = cache["data"].setdefault(instrument, {})
    if unit in entry:
        return entry[unit]
    if unit == "hours":
        candles = fetch_hist(instrument, "hours", HIST_HOURS_DAYS, today_d)
        keep = KEEP_HOURS
    else:
        candles = fetch_hist(instrument, "days", HIST_DAYS_DAYS, today_d)
        keep = KEEP_DAYS
    if candles is None:  # error: cache mat karo, agli baar dobara try hoga
        return None
    entry[unit] = last_pairs(candles, unit, keep)
    return entry[unit]


# ---------------------------------------------------------------- scan
def scan_timeframe(label, unit, stocks, cache, today_d, today_s, now, already):
    rows = []
    for symbol, instrument in stocks:
        if symbol in already:  # aaj aa chuka hai, API call bhi bacha lo
            continue
        hist = get_hist(cache, instrument, unit, today_d)
        if hist is None:
            continue
        intraday = fetch_intraday(instrument, unit)
        if not intraday:
            continue
        series = merged_series(hist, intraday, unit)
        if not series or series[-1][0][:10] != today_s:  # aaj ka data nahi (holiday / suspended)
            continue
        value = rsi([v for _, v in series])
        if value is None:
            continue
        if value > UPPER:
            signal = "RSI 80+ (Overbought)"
        elif value < LOWER:
            signal = "RSI 20- (Oversold)"
        else:
            continue
        rows.append([today_s, now.strftime("%H:%M"), symbol, label,
                     round(value, 1), round(series[-1][1], 2), signal])
        already.add(symbol)
    return rows


def open_sheet():
    import gspread
    from gspread.exceptions import WorksheetNotFound

    if not SHEET_ID or not GOOGLE_CREDENTIALS:
        raise SystemExit("SHEET_ID ya GOOGLE_CREDENTIALS secret set nahi hai.")
    client = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS))
    book = client.open_by_key(SHEET_ID)
    try:
        return book.worksheet(TAB_NAME)
    except WorksheetNotFound:
        raise SystemExit("Sheet mein '%s' naam ka tab nahi mila." % TAB_NAME)


def main():
    now = datetime.now(IST)
    today_d = now.date()
    today_s = today_d.isoformat()
    mins = now.hour * 60 + now.minute

    if not FORCE:
        if now.weekday() >= 5:
            print("Weekend, skip.")
            return
        if mins < 8 * 60 + 30 or mins > 15 * 60 + 35:
            print("Market time ke bahar, skip.")
            return

    stocks = load_nifty500()
    cache = load_cache(today_s)

    if not FORCE and mins < 9 * 60 + 15:  # pre-market: sirf history tayyar karo
        for _, instrument in stocks:
            get_hist(cache, instrument, "hours", today_d)
            get_hist(cache, instrument, "days", today_d)
        save_cache(cache)
        print("History cache tayyar: %d stocks." % len(cache["data"]))
        return

    ws = open_sheet()
    # Header har run mein pakka likho (khali sheet par gspread [[]] deta hai, isliye "khali hai kya" check bharosemand nahi)
    ws.update(range_name="A1:G1", values=[HEADER], value_input_option="RAW")
    values = ws.get_all_values() or []
    already = {r[2].strip() for r in values[1:] if len(r) > 2 and r[0].strip() == today_s}

    new_rows = []
    if MODE in ("hourly", "both"):
        new_rows += scan_timeframe("1 HOUR", "hours", stocks, cache, today_d, today_s, now, already)
    if MODE in ("daily", "both"):
        new_rows += scan_timeframe("1 DAY", "days", stocks, cache, today_d, today_s, now, already)

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW", table_range="A1")
    # H1 mein "last scan" likho, taaki sheet mein dikhe ki scanner zinda hai
    ws.update(range_name="H1",
              values=[["Last scan: %s %s IST | %s | naye: %d" % (today_s, now.strftime("%H:%M"), MODE, len(new_rows))]],
              value_input_option="RAW")
    save_cache(cache)
    print("Scan complete: %d naye stock sheet mein jode." % len(new_rows))


if __name__ == "__main__":
    main()
