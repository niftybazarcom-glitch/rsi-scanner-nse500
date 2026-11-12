"""
EMA DAILY scanner: Nifty 500 (Upstox 1 DAY data) -> Google Sheet tab "EMA DAILY" + Telegram alert

Setup = "EMA pullback + retest breakout", sirf 1 DAY candle, din mein EK baar, market band hone ke baad:

1. Impulse : price pehli baar sabhi 4 EMAs (20/50/100/200) ke upar close hua aur kam se kam 3% badha.
             (Yahan entry NAHI.)
2. Pullback: wahan se gira aur kisi bhi EMA (20/50/100/200) tak aaya (candle ke low ne EMA ko touch kiya).
             Beech ke EMAs ke neeche close ho jaye to bhi theek. Koi % tolerance nahi.
             Sheet ke "Support" column mein likha aata hai ki low ne kaun si EMA ko (sabse gehri) touch kiya.
3. Entry   : pullback ke low ke baad (max 6 candles ke andar) ek bullish candle jo sabhi 4 EMAs ke upar close kare.
             Candle type: Lambi green body, Bullish engulfing, Gap-up green, ya Hammer.
4. Extra   : volume >= 1.5x (pichle 20 din ka average), RSI(14) 55 se 70, MACD(12,26,9) bullish crossover
             (aaj ya pichli 3 candles mein), close Rs 250 se Rs 4000 ke beech.
5. Tag     : chaaron EMAs ka spread = (sabse upar - sabse neeche) / close. 5% ya kam ho to "TIGHT" (paas-paas).
             TIGHT wale sheet aur Telegram mein pehle aate hain. Wide wale hataye nahi jaate.

Ek hi signal baar-baar nahi aata: pichli 6 candles mein wahi setup pehle ban chuka ho to skip.
Din mein ek hi baar chalta hai (sheet ke M1 mein DONE likha ho to dobara skip), FORCE=true se test kar sakte ho.
"""
import csv
import io
import json
import os
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import requests

IST = timezone(timedelta(hours=5, minutes=30))
HIST_URL = "https://api.upstox.com/v3/historical-candle"

UPSTOX_TOKEN = os.environ.get("UPSTOX_TOKEN", "")
SHEET_ID = os.environ.get("SHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FORCE = os.environ.get("FORCE", "").strip().lower() in ("1", "true", "yes")

TAB = "EMA DAILY"
HEADER = ["Date", "Stock", "Close", "Candle type", "Support", "EMA spread %", "Tight",
          "Vol x avg", "RSI", "Impulse gain %", "Scan time"]
STATUS_CELL = "M1"

# ---- rules (yahan se badal sakte ho)
PRICE_MIN = 250
PRICE_MAX = 4000
EMA_PERIODS = (20, 50, 100, 200)
IMPULSE_MIN_PCT = 3.0        # sabhi EMAs ke upar aane ke baad kam se kam itna % badhna chahiye
LOOKBACK = 60                # impulse ka peak + pullback itne pichle candles mein dhoondho
MAX_AFTER_LOW = 6            # pullback ke low ke baad itni candles ke andar entry candle
BODY_MIN = 0.5               # lambi green: body >= range ka 50%
CLOSE_POS_MIN = 0.7          # close candle range ke upar ke 30% mein
HAMMER_WICK_X = 2.0          # hammer: neeche ki wick >= body ki 2 guna
HAMMER_UPPER_MAX = 0.15      # hammer: upar ki wick <= range ka 15%
VOL_MULT = 1.5
VOL_AVG_N = 20
RSI_MIN, RSI_MAX = 55.0, 70.0
MACD_CROSS_BACK = 3
TIGHT_PCT = 5.0
MIN_CANDLES = 250            # EMA 200 ke liye
WARMUP = 200                 # isse pehle ke candles par EMA bharosemand nahi
HIST_DAYS_TRY = (900, 600, 400)   # calendar days; pehla fail ho to agla try hota hai

MAX_PER_MIN = 400            # Upstox limit (500/min) se neeche
MAX_PER_30MIN = 1500         # Upstox limit (2000 per 30 min) se neeche
MIN_GAP = 0.1
LIMIT_SLEEP_MAX = 120

NIFTY500_URLS = [
    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
    "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
]


class LimitReached(Exception):
    pass


def now_ist():
    return datetime.now(IST)


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


def fetch_hist(key, unit, interval, days_back, today_d):
    frm = (today_d - timedelta(days=days_back)).isoformat()
    j = api_get("%s/%s/%s/%s/%s/%s" % (HIST_URL, quote(key, safe=""), unit, interval, today_d.isoformat(), frm))
    if j is None:
        return None
    return (j.get("data") or {}).get("candles") or []


_RANGE = {"i": 0, "confirmed": False}


def fetch_daily(key, today_d):
    """1 DAY candles. Lamba range reject ho to chhota range try karta hai (pehli baar hi)."""
    while True:
        days = HIST_DAYS_TRY[_RANGE["i"]]
        c = fetch_hist(key, "days", 1, days, today_d)
        if c:
            _RANGE["confirmed"] = True
            return c
        if c is None and not _RANGE["confirmed"] and _RANGE["i"] + 1 < len(HIST_DAYS_TRY):
            _RANGE["i"] += 1
            continue
        return c


# ---------------------------------------------------------------- candles + indicators
def parse_candles(candles):
    """Upstox candle [time, o, h, l, c, v, oi] -> [(date, o, h, l, c, v)] purana se naya."""
    rows = {}
    for c in candles or []:
        try:
            try:
                ts = datetime.fromisoformat(str(c[0]))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=IST)
            except ValueError:
                ts = datetime.fromtimestamp(int(c[0]) / 1000, IST)
            d = ts.astimezone(IST).date().isoformat()
            o, h, l, cl = float(c[1]), float(c[2]), float(c[3]), float(c[4])
            v = float(c[5] or 0)
        except (IndexError, TypeError, ValueError):
            continue
        if min(o, h, l, cl) <= 0:
            continue
        rows[d] = (d, o, h, l, cl, v)
    return [rows[k] for k in sorted(rows)]


def ema_series(values, n):
    a = 2.0 / (n + 1)
    out, e = [], None
    for v in values:
        e = v if e is None else a * v + (1 - a) * e
        out.append(e)
    return out


def rsi_series(closes, n=14):
    """Wilder RSI (TradingView jaisa)."""
    out = [None] * len(closes)
    if len(closes) <= n:
        return out

    def val(ag, al):
        if al == 0:
            return 100.0 if ag > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + ag / al)

    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    ag, al = gains / n, losses / n
    out[n] = val(ag, al)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
        out[i] = val(ag, al)
    return out


class Data:
    """Ek stock ka daily data + indicators."""

    def __init__(self, rows):
        self.date = [r[0] for r in rows]
        self.o = [r[1] for r in rows]
        self.h = [r[2] for r in rows]
        self.l = [r[3] for r in rows]
        self.c = [r[4] for r in rows]
        self.v = [r[5] for r in rows]
        n = len(self.c)
        self.emas = {p: ema_series(self.c, p) for p in EMA_PERIODS}
        self.top = [max(self.emas[p][i] for p in EMA_PERIODS) for i in range(n)]
        self.bot = [min(self.emas[p][i] for p in EMA_PERIODS) for i in range(n)]
        self.up = [self.c[i] > self.top[i] for i in range(n)]   # close sabhi 4 EMAs ke upar
        self.rsi = rsi_series(self.c, 14)
        e12, e26 = ema_series(self.c, 12), ema_series(self.c, 26)
        self.macd = [a - b for a, b in zip(e12, e26)]
        self.sig = ema_series(self.macd, 9)


# ---------------------------------------------------------------- setup ke tukde
def candle_types(d, i):
    """Entry candle ke type (list). Khali list = koi type nahi. Sabhi 4 EMAs ke upar close ho (caller check karta hai)."""
    o, h, l, c = d.o[i], d.h[i], d.l[i], d.c[i]
    rng = h - l
    if rng <= 0 or i < 1:
        return []
    body = abs(c - o)
    pos = (c - l) / rng
    green = c > o
    out = []
    if green and body / rng >= BODY_MIN and pos >= CLOSE_POS_MIN and c > d.h[i - 1]:
        out.append("Lambi green")
    if green and d.c[i - 1] < d.o[i - 1] and o <= d.c[i - 1] and c >= d.o[i - 1]:
        out.append("Bullish engulfing")
    if green and o > d.h[i - 1]:
        out.append("Gap-up green")
    lower = min(o, c) - l
    upper = h - max(o, c)
    if (lower >= HAMMER_WICK_X * max(body, rng * 0.02) and upper <= HAMMER_UPPER_MAX * rng
            and pos >= CLOSE_POS_MIN and l <= d.top[i]):
        out.append("Hammer")
    return out


def macd_cross(d, i):
    if d.macd[i] <= d.sig[i]:
        return False
    for j in range(max(1, i - MACD_CROSS_BACK), i + 1):
        if d.macd[j - 1] <= d.sig[j - 1] and d.macd[j] > d.sig[j]:
            return True
    return False


def pullback_info(d, i):
    """Impulse (>=3%) -> pullback (EMA tak) -> abhi i par entry. Dict ya None."""
    start = max(WARMUP, i - LOOKBACK)
    if i - start < 3:
        return None
    # peak: i se pehle ka sabse ooncha high
    P = start
    for k in range(start, i):
        if d.h[k] >= d.h[P]:
            P = k
    # impulse: P tak ka "sabhi EMAs ke upar" daur kab shuru hua (pehla close upar)
    lo = max(WARMUP, P - 300)
    X = None
    for k in range(P, lo - 1, -1):
        if d.up[k] and (k == 0 or not d.up[k - 1]):
            X = k
            break
    if X is None and d.up[lo]:
        X = lo
    if X is None:
        return None
    gain = (d.h[P] / d.c[X] - 1.0) * 100.0
    if gain < IMPULSE_MIN_PCT:
        return None
    # pullback low: peak ke baad (i tak) sabse gehra low
    T = P + 1
    for k in range(P + 1, i + 1):
        if d.l[k] <= d.l[T]:
            T = k
    if d.l[T] > d.top[T]:
        return None                      # EMA tak aaya hi nahi
    if i - T > MAX_AFTER_LOW:
        return None
    touched = [p for p in EMA_PERIODS if d.l[T] <= d.emas[p][T]]
    deepest = min(touched, key=lambda p: d.emas[p][T])
    label = "EMA%d" % deepest + (" (neeche tak)" if d.l[T] < d.bot[T] else "")
    return {"P": P, "X": X, "T": T, "gain": gain, "support": label}


def qualifies(d, i):
    if i < MIN_CANDLES - 1 or i < 1:
        return None
    c = d.c[i]
    if not (PRICE_MIN <= c <= PRICE_MAX):
        return None
    if not d.up[i]:
        return None
    types = candle_types(d, i)
    if not types:
        return None
    r = d.rsi[i]
    if r is None or not (RSI_MIN <= r <= RSI_MAX):
        return None
    if not macd_cross(d, i):
        return None
    window = d.v[i - VOL_AVG_N:i]
    avg = sum(window) / len(window) if window else 0.0
    if avg <= 0 or d.v[i] / avg < VOL_MULT:
        return None
    pb = pullback_info(d, i)
    if not pb:
        return None
    spread = (d.top[i] - d.bot[i]) / c * 100.0
    return {"close": c, "types": " + ".join(types), "support": pb["support"], "spread": spread,
            "tight": spread <= TIGHT_PCT, "vol": d.v[i] / avg, "rsi": r, "gain": pb["gain"]}


def signal_today(d, i):
    """Aaj (candle i) ka naya signal. Pichli 6 candles mein wahi setup ban chuka ho to None."""
    q = qualifies(d, i)
    if not q:
        return None
    for j in range(max(MIN_CANDLES - 1, i - MAX_AFTER_LOW), i):
        if qualifies(d, j):
            return None
    return q


# ---------------------------------------------------------------- stock list, sheet, telegram
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


def open_book():
    import gspread

    if not SHEET_ID or not GOOGLE_CREDENTIALS:
        raise SystemExit("SHEET_ID ya GOOGLE_CREDENTIALS secret set nahi hai.")
    client = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS))
    return client.open_by_key(SHEET_ID)


def get_ws(book, title, rows=1000, cols=14):
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


def telegram_text(signals):
    def line(s):
        return "• %s | Close %s | %s | Support %s | spread %.1f%% | Vol %.1fx | RSI %.0f" % (
            s["stock"], round(s["close"], 2), s["types"], s["support"], s["spread"], s["vol"], s["rsi"])

    tight = [s for s in signals if s["tight"]]
    rest = [s for s in signals if not s["tight"]]
    out = "📊 EMA DAILY (pullback + breakout): %d stock\n" % len(signals)
    if tight:
        out += "\nTIGHT (EMAs paas-paas):\n" + "\n".join(line(s) for s in tight) + "\n"
    if rest:
        out += "\nBaaki:\n" + "\n".join(line(s) for s in rest) + "\n"
    return out


# ---------------------------------------------------------------- scan
def run_scan(stocks, now, ws):
    today_s = now.date().isoformat()
    today_d = now.date()
    st = {"checked": 0, "no_today": 0, "short": 0, "no_data": 0}
    signals, partial = [], False
    try:
        for sym, key in stocks:
            candles = fetch_daily(key, today_d)
            if candles is None:
                st["no_data"] += 1
                if st["no_data"] >= 25 and st["checked"] + st["short"] + st["no_today"] == 0:
                    raise SystemExit("Upstox se daily data nahi aa raha (25 stocks fail). Token ya range check karo.")
                continue
            rows = parse_candles(candles)
            if len(rows) < MIN_CANDLES:
                st["short"] += 1
                continue
            if not FORCE and rows[-1][0] != today_s:
                st["no_today"] += 1          # aaj ka daily candle nahi (holiday ya data abhi nahi aaya)
                continue
            d = Data(rows)
            st["checked"] += 1
            q = signal_today(d, len(rows) - 1)
            if q:
                q["stock"], q["date"] = sym, rows[-1][0]
                signals.append(q)
    except LimitReached:
        partial = True
        print("Upstox limit ki wajah se scan beech mein ruka.")

    signals.sort(key=lambda s: (not s["tight"], s["spread"]))
    seen = {(r[0], r[1]) for r in (ws.get_all_values() or [])[1:] if len(r) > 1}
    new = [s for s in signals if (s["date"], s["stock"]) not in seen]
    if new:
        ws.append_rows(
            [[s["date"], s["stock"], round(s["close"], 2), s["types"], s["support"], round(s["spread"], 1),
              "TIGHT" if s["tight"] else "", round(s["vol"], 1), round(s["rsi"], 1), round(s["gain"], 1),
              now.strftime("%H:%M")] for s in new],
            value_input_option="RAW", table_range="A1")
        send_telegram(telegram_text(new))
    # DONE tabhi likhte hain jab kuch check hua ho; aaj ka candle hi na mila (holiday / data nahi aaya) to dobara chal sakta hai
    state = "PARTIAL" if partial else ("NO-CANDLE" if st["checked"] == 0 and st["no_today"] > 0 else "DONE")
    ws.update(range_name=STATUS_CELL, values=[[
        "Last scan: %s %s IST | %s | stocks: %d | checked: %d | aaj ka candle nahi: %d | history kam: %d | "
        "data nahi mila: %d | naye signal: %d | Upstox calls: %d" % (
            today_s, now.strftime("%H:%M"), state, len(stocks), st["checked"],
            st["no_today"], st["short"], st["no_data"], len(new), LIMITER.total)]], value_input_option="RAW")
    print("Scan: checked=%d naye=%d calls=%d partial=%s" % (st["checked"], len(new), LIMITER.total, partial))


def already_done(ws, today_s):
    try:
        val = ws.acell(STATUS_CELL).value or ""
    except Exception:
        return False
    return today_s in val and "| DONE |" in val


def main():
    now = now_ist()
    mins = now.hour * 60 + now.minute
    if not FORCE:
        if now.weekday() >= 5:
            print("Weekend, skip.")
            return
        if mins < 15 * 60 + 40:
            print("Market abhi band nahi hua (3:40 PM ke baad chalega), skip.")
            return
    stocks = load_nifty500()
    ws = get_ws(open_book(), TAB)
    if not FORCE and already_done(ws, now.date().isoformat()):
        print("Aaj ka scan ho chuka hai, skip.")
        return
    ws.update(range_name="A1:K1", values=[HEADER], value_input_option="RAW")
    run_scan(stocks, now, ws)


if __name__ == "__main__":
    main()
