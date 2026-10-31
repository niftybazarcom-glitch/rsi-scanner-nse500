"""
EMA SETUP scanner: Nifty 500 (Upstox data) -> Google Sheet tab "EMA SETUP" + Telegram alert

Rule (option Y):
- BUY : 15 MIN ki pichli band candle ne price ko sabhi 4 EMAs (20/50/100/200) ke UPAR band kiya hai
        aur usse pehle ki 3 candles mein se kam se kam ek mein price kisi EMA ke neeche/barabar tha (fresh crossover),
        AUR 1 HOUR ki pichli band candle bhi sabhi 4 EMAs ke upar hai.
- SELL: bilkul ulta (sabhi EMAs ke neeche).
- Dono timeframe ulte hon to stock list mein nahi aata.
- Sirf BAND hui candle par signal.

Stocks: nifty500.csv, prev close Rs 250 se Rs 5000, circuit wale hata diye (Upstox ke quote ki circuit limit se).

Calls bachane ke liye "funnel": 1 HOUR ka haal har ghante ek baar (candle band hone par) sab stocks ka nikalta hai,
phir 15 MIN scan sirf unhi stocks par chalta hai jinka 1 HOUR haal BUY ya SELL hai.
1 HOUR candles 15 MIN candles se banti hain (9:15 se shuru), isliye ek stock ke liye ek hi intraday call lagti hai.
"""
import csv
import gzip
import io
import json
import os
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests

IST = timezone(timedelta(hours=5, minutes=30))
HIST_URL = "https://api.upstox.com/v3/historical-candle"
QUOTE_URL = "https://api.upstox.com/v3/market-quote/quotes"

UPSTOX_TOKEN = os.environ.get("UPSTOX_TOKEN", "")
SHEET_ID = os.environ.get("SHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MODE = os.environ.get("MODE", "scan")  # prep | scan
FORCE = os.environ.get("FORCE", "").strip().lower() in ("1", "true", "yes")

TAB_SETUP = "EMA SETUP"
TAB_LIST = "EMA 1H LIST"
SETUP_HEADER = ["Date", "Time", "Stock", "Signal", "Close", "15M candle", "1H candle", "Note"]
LIST_HEADER = ["Stock", "Direction", "1H candle", "Updated"]

PRICE_MIN = 250
PRICE_MAX = 5000
EMA_PERIODS = (20, 50, 100, 200)
FRESH_BACK = 3               # pichli itni candles mein price kisi EMA ke "galat side" tha
MIN_CANDLES = 220            # EMA 200 ke liye kam se kam itni candles
BAND_MAX_PCT = 10.5          # circuit band itna ya usse kam ho to stock hata do
CHECK_CIRCUIT_HISTORY = True # pichle 6 mahine mein 2+ baar +-20% circuit-jaisa din ho to hata do
HIST_15M_DAYS = 30
HIST_1H_DAYS = 80
HIST_1D_DAYS = 200
KEEP_15M = 600
KEEP_1H = 500
CACHE_FILE = "ema_cache.json.gz"

MAX_PER_MIN = 400            # Upstox ki limit (500/min) se neeche
MAX_PER_30MIN = 1500         # Upstox ki limit (2000 per 30 min) se neeche, dusre scripts ke liye jagah
MIN_GAP = 0.1
LIMIT_SLEEP_MAX = 120        # isse zyada rukna pade to is run ko wahin rok do

NIFTY500_URLS = [
    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
]


class LimitReached(Exception):
    pass


# ---------------------------------------------------------------- time helpers
def now_ist():
    return datetime.now(IST)


def to_epoch(ts):
    s = str(ts)
    try:
        return int(datetime.fromisoformat(s).timestamp())
    except ValueError:
        return int(int(s) / 1000)


def from_epoch(e):
    return datetime.fromtimestamp(e, IST)


def candle_end(start, minutes):
    end = start + timedelta(minutes=minutes)
    sess_end = start.replace(hour=15, minute=30, second=0, microsecond=0)
    return min(end, sess_end) if start < sess_end else end


def is_closed(start, minutes, now):
    return now >= candle_end(start, minutes)


def hour_bucket(start):
    base = start.replace(hour=9, minute=15, second=0, microsecond=0)
    k = int((start - base).total_seconds() // 3600)
    return base + timedelta(hours=k)


def latest_closed_hour(now):
    """Aaj ki sabse nayi band hui 1 HOUR candle ka start time (9:15 se har ghante), warna None."""
    base = now.replace(hour=9, minute=15, second=0, microsecond=0)
    best = None
    for k in range(0, 7):
        start = base + timedelta(hours=k)
        if is_closed(start, 60, now):
            best = start
    return best


# ---------------------------------------------------------------- API (rate limit ke saath)
class Limiter:
    def __init__(self):
        self.calls = deque()
        self.total = 0
        self.last = 0.0

    def wait(self):
        while True:
            now = time.time()
            while self.calls and now - self.calls[0] > 1800:
                self.calls.popleft()
            recent = [t for t in self.calls if now - t <= 60]
            if len(recent) >= MAX_PER_MIN:
                need = 60 - (now - recent[0]) + 0.5
            elif len(self.calls) >= MAX_PER_30MIN:
                need = 1800 - (now - self.calls[0]) + 0.5
            else:
                break
            if need > LIMIT_SLEEP_MAX:
                raise LimitReached()
            time.sleep(need)
        gap = MIN_GAP - (time.time() - self.last)
        if gap > 0:
            time.sleep(gap)
        self.last = time.time()
        self.calls.append(self.last)
        self.total += 1


LIMITER = Limiter()


def api_get(url):
    """JSON dict return karta hai, error par None."""
    headers = {"Accept": "application/json", "Authorization": "Bearer " + UPSTOX_TOKEN}
    for attempt in range(4):
        LIMITER.wait()
        try:
            r = requests.get(url, headers=headers, timeout=20)
        except requests.RequestException:
            time.sleep(2)
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                return None
        if r.status_code == 429:
            time.sleep(10 * (attempt + 1))
            continue
        if r.status_code in (401, 403):
            raise SystemExit("Upstox ne token reject kiya (HTTP %s). UPSTOX_TOKEN check karo." % r.status_code)
        return None
    return None


def enc(key):
    return quote(key, safe="")


def fetch_hist(key, unit, interval, days_back, today_d):
    frm = (today_d - timedelta(days=days_back)).isoformat()
    j = api_get("%s/%s/%s/%s/%s/%s" % (HIST_URL, enc(key), unit, interval, today_d.isoformat(), frm))
    if j is None:
        return None
    return (j.get("data") or {}).get("candles") or []


def fetch_intraday_15m(key):
    j = api_get("%s/intraday/%s/minutes/15" % (HIST_URL, enc(key)))
    if j is None:
        return None
    return (j.get("data") or {}).get("candles") or []


def fetch_quotes(keys):
    out = {}
    for i in range(0, len(keys), 100):
        part = keys[i:i + 100]
        j = api_get(QUOTE_URL + "?instrument_key=" + ",".join(enc(k) for k in part))
        if j is None:
            continue
        for _, q in (j.get("data") or {}).items():
            tok = (q or {}).get("instrument_token")
            if tok:
                out[tok] = q
    return out


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------- EMA
def ema_series(values, n):
    a = 2.0 / (n + 1)
    out, e = [], None
    for v in values:
        e = v if e is None else a * v + (1 - a) * e
        out.append(e)
    return out


def flags_for(closes):
    """Har candle ke liye: 'UP' (sabhi 4 EMAs ke upar), 'DOWN' (sabhi ke neeche), warna 'MIX'."""
    if len(closes) < MIN_CANDLES:
        return None
    emas = [ema_series(closes, n) for n in EMA_PERIODS]
    flags = []
    for i, c in enumerate(closes):
        hi = max(e[i] for e in emas)
        lo = min(e[i] for e in emas)
        flags.append("UP" if c > hi else ("DOWN" if c < lo else "MIX"))
    return flags


def fresh(f15, direction):
    want = "UP" if direction == "BUY" else "DOWN"
    return f15[-1] == want and any(f != want for f in f15[-1 - FRESH_BACK:-1])


# ---------------------------------------------------------------- candles ko jodna
def compact(candles, keep):
    rows = {}
    for c in candles or []:
        try:
            rows[to_epoch(c[0])] = float(c[4])
        except (IndexError, TypeError, ValueError):
            continue
    items = sorted(rows.items())[-keep:]
    return [[e, v] for e, v in items]


def analyze(entry, intraday, now):
    """Ek stock ka 15 MIN aur 1 HOUR haal (sirf band hui candles). Aaj ka data na ho to None."""
    d = {e: c for e, c in entry["h15"]}
    for cd in intraday or []:
        try:
            d[to_epoch(cd[0])] = float(cd[4])
        except (IndexError, TypeError, ValueError):
            continue
    items = []
    for e, c in d.items():
        st = from_epoch(e)
        if is_closed(st, 15, now):
            items.append((st, c))
    items.sort(key=lambda x: x[0])
    if not items or items[-1][0].date() != now.date():
        return None
    f15 = flags_for([c for _, c in items])

    hourly = {e: c for e, c in entry["h1"] if from_epoch(e).date() < now.date()}
    buckets = {}
    for st, c in items:
        if st.date() != now.date():
            continue
        b = hour_bucket(st)
        if b < st.replace(hour=9, minute=15, second=0, microsecond=0):
            continue
        buckets[b] = c  # ek ghante ki aakhri 15 MIN candle ka close
    for b, c in buckets.items():
        if is_closed(b, 60, now):
            hourly[int(b.timestamp())] = c
    h_items = sorted(hourly.items())
    f1 = flags_for([c for _, c in h_items])
    if f15 is None or f1 is None:
        return None
    t1 = from_epoch(h_items[-1][0])
    if t1.date() != now.date():
        return None  # aaj ki koi 1 HOUR candle abhi band nahi hui
    return {"f15": f15, "f1": f1, "close15": items[-1][1], "t15": items[-1][0], "t1": t1}


def direction_of(res):
    if res is None:
        return None
    return {"UP": "BUY", "DOWN": "SELL"}.get(res["f1"][-1])


# ---------------------------------------------------------------- stock list, prep
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
        raise SystemExit("Nifty 500 list nahi mili. Repo mein 'nifty500.csv' upload karo.")
    stocks = []
    for row in csv.DictReader(io.StringIO(text)):
        row = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
        symbol, isin = row.get("Symbol"), row.get("ISIN Code")
        if symbol and isin:
            stocks.append((symbol, "NSE_EQ|" + isin))
    if not stocks:
        raise SystemExit("Nifty 500 file padhi nahi gayi (Symbol / ISIN Code columns chahiye).")
    return stocks


def circuit_days(candles):
    rows = sorted(((to_epoch(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4])) for c in candles or []))
    n = 0
    for i in range(max(1, len(rows) - 130), len(rows)):
        prev = rows[i - 1][4]
        _, o, h, l, c = rows[i]
        if prev <= 0:
            continue
        chg = (c - prev) / prev * 100
        if chg >= 19.5 and c >= h * 0.999:
            n += 1
        elif chg <= -19.5 and c <= l * 1.001:
            n += 1
    return n


def run_prep(stocks, now):
    today_d = now.date()
    cache = {"date": today_d.isoformat(), "stocks": {}, "excluded": {}, "partial": False}
    ex = cache["excluded"]
    quotes = fetch_quotes([k for _, k in stocks])
    eligible = []
    for sym, key in stocks:
        q = quotes.get(key)
        if not q:
            ex[sym] = "quote nahi mila"
            continue
        prev = num(q.get("prev_close_price")) or num(q.get("reference_price"))
        px = num(q.get("last_price"))
        if prev <= 0:
            ex[sym] = "prev close nahi"
            continue
        if prev < PRICE_MIN or prev > PRICE_MAX:
            ex[sym] = "price %.0f" % prev
            continue
        up, lo = num(q.get("upper_circuit_limit")), num(q.get("lower_circuit_limit"))
        ref = num(q.get("reference_price")) or prev
        if up > 0 and ref > 0:   # F&O stocks mein aksar koi band nahi hota (up = 0): wo rakhe jayenge
            band = (up - ref) / ref * 100
            if band <= BAND_MAX_PCT:
                ex[sym] = "circuit band %.0f%%" % band
                continue
            if px > 0 and (px >= up * 0.9995 or (lo > 0 and px <= lo * 1.0005)):
                ex[sym] = "abhi circuit par"
                continue
        eligible.append((sym, key))
    try:
        for sym, key in eligible:
            if CHECK_CIRCUIT_HISTORY:
                days = fetch_hist(key, "days", 1, HIST_1D_DAYS, today_d)
                if days is not None and circuit_days(days) >= 2:
                    ex[sym] = "pichle 6 mahine mein 2+ circuit-jaise din"
                    continue
            h15 = fetch_hist(key, "minutes", 15, HIST_15M_DAYS, today_d)
            h1 = fetch_hist(key, "hours", 1, HIST_1H_DAYS, today_d)
            if h15 is None or h1 is None:
                ex[sym] = "history nahi mili"
                continue
            cache["stocks"][sym] = {"key": key, "h15": compact(h15, KEEP_15M), "h1": compact(h1, KEEP_1H)}
    except LimitReached:
        cache["partial"] = True
        print("Upstox limit ki wajah se prep beech mein ruka.")
    print("Prep: %d stocks rakhe, %d hataye (price/circuit/data), calls: %d" %
          (len(cache["stocks"]), len(ex), LIMITER.total))
    return cache


def load_cache(today_s):
    try:
        with gzip.open(CACHE_FILE, "rt", encoding="utf-8") as f:
            c = json.load(f)
        if c.get("date") == today_s:
            return c
    except Exception:
        pass
    return None


def save_cache(cache):
    with gzip.open(CACHE_FILE, "wt", encoding="utf-8") as f:
        json.dump(cache, f)


# ---------------------------------------------------------------- sheet, telegram
def open_book():
    import gspread

    if not SHEET_ID or not GOOGLE_CREDENTIALS:
        raise SystemExit("SHEET_ID ya GOOGLE_CREDENTIALS secret set nahi hai.")
    client = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS))
    return client.open_by_key(SHEET_ID)


def get_ws(book, title, rows=1000, cols=12):
    from gspread.exceptions import WorksheetNotFound

    try:
        return book.worksheet(title)
    except WorksheetNotFound:
        return book.add_worksheet(title=title, rows=rows, cols=cols)


def send_telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("TELEGRAM_TOKEN / TELEGRAM_CHAT_ID set nahi hain, alert nahi bheja.")
        return False
    url = "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN
    ok = True
    for i in range(0, len(text), 3800):
        try:
            r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text[i:i + 3800]}, timeout=20)
            ok = ok and r.status_code == 200
        except requests.RequestException:
            ok = False
    if not ok:
        print("Telegram alert bhejna fail hua.")
    return ok


# ---------------------------------------------------------------- scan
def run_scan(cache, now, book):
    today_s = now.date().isoformat()
    ws_setup = get_ws(book, TAB_SETUP, cols=12)
    ws_list = get_ws(book, TAB_LIST, cols=8)
    ws_setup.update(range_name="A1:H1", values=[SETUP_HEADER], value_input_option="RAW")

    def status(msg):
        ws_setup.update(range_name="J1", values=[["Last scan: %s %s IST | %s | Upstox calls: %d" %
                                                  (today_s, now.strftime("%H:%M"), msg, LIMITER.total)]],
                        value_input_option="RAW")

    t1 = latest_closed_hour(now)
    if t1 is None:
        status("1H candle abhi band nahi hui (pehli 10:15 par)")
        return
    t1_key = t1.isoformat()

    values = ws_list.get_all_values() or []
    saved_key = values[0][5] if values and len(values[0]) > 5 else ""
    refresh = saved_key != t1_key
    stocks = cache["stocks"]
    if refresh:
        todo = list(stocks.items())
    else:
        listed = {r[0] for r in values[1:] if r and r[0]}
        todo = [(s, e) for s, e in stocks.items() if s in listed]

    cands, signals, checked = {}, [], 0
    try:
        for sym, entry in todo:
            intraday = fetch_intraday_15m(entry["key"])
            if intraday is None:
                continue
            checked += 1
            res = analyze(entry, intraday, now)
            d = direction_of(res)
            if not d:
                continue
            cands[sym] = d
            if fresh(res["f15"], d):
                signals.append({"stock": sym, "dir": d, "close": res["close15"],
                                "t15": res["t15"].strftime("%H:%M"), "t1": res["t1"].strftime("%H:%M")})
    except LimitReached:
        print("Upstox limit ki wajah se scan beech mein ruka.")

    if refresh:
        rows = [[s, d, t1.strftime("%H:%M"), now.strftime("%H:%M")] for s, d in sorted(cands.items())]
        ws_list.clear()
        ws_list.update(range_name="A1", values=[LIST_HEADER + ["key", t1_key]] + rows, value_input_option="RAW")

    seen = {(r[2], r[3]) for r in (ws_setup.get_all_values() or [])[1:] if len(r) > 3 and r[0] == today_s}
    new = [s for s in signals if (s["stock"], s["dir"]) not in seen]
    if new:
        ws_setup.append_rows(
            [[today_s, now.strftime("%H:%M"), s["stock"], s["dir"], round(s["close"], 2), s["t15"], s["t1"],
              "15M fresh crossover + 1H sabhi EMAs ke " + ("upar" if s["dir"] == "BUY" else "neeche")] for s in new],
            value_input_option="RAW", table_range="A1")
        for d, icon in (("BUY", "📈"), ("SELL", "📉")):
            part = [s for s in new if s["dir"] == d]
            if part:
                send_telegram("%s EMA SETUP: %s\n" % (icon, d) + "\n".join(
                    "• %s | Close %s | 15M %s | 1H %s" % (s["stock"], round(s["close"], 2), s["t15"], s["t1"])
                    for s in part))
    status("%s | 1H list: %d | checked: %d | naye signal: %d" %
           ("1H refresh" if refresh else "sirf 15M", len(cands) if refresh else len(todo), checked, len(new)))
    print("Scan: refresh=%s checked=%d naye=%d calls=%d" % (refresh, checked, len(new), LIMITER.total))


def main():
    now = now_ist()
    today_s = now.date().isoformat()
    mins = now.hour * 60 + now.minute
    if not FORCE:
        if now.weekday() >= 5:
            print("Weekend, skip.")
            return
        if mins < 8 * 60 or mins > 15 * 60 + 40:
            print("Market time ke bahar, skip.")
            return

    stocks = load_nifty500()
    cache = load_cache(today_s)
    if MODE == "prep" or cache is None:
        cache = run_prep(stocks, now)
        save_cache(cache)
        if MODE == "prep":
            return
    if not FORCE and mins < 9 * 60 + 15:
        print("Market abhi khula nahi, scan skip.")
        return
    run_scan(cache, now, open_book())


if __name__ == "__main__":
    main()
