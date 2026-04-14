
"""
Nifty 50 — Live 1-Min Candle Chart via AngelOne SmartAPI
Usage:
    python nifty_live_candles.py          # fetch & plot once
    python nifty_live_candles.py live     # auto-refresh every 60 s
    python nifty_live_candles.py live 30  # auto-refresh every 30 s
"""

import os
import sys
import time
import json
import logging
import webbrowser
from datetime import datetime

import numpy as np
import pyotp
import pytz
import pandas as pd
from SmartApi import SmartConnect
from dotenv import load_dotenv

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Credentials ────────────────────────────────────────────────────────────
load_dotenv()
API_KEY     = os.getenv("API_KEY")
CLIENT_ID   = os.getenv("CLIENT_ID")
PASSWORD    = os.getenv("PASSWORD")
TOTP_SECRET = os.getenv("TOTP_SECRET") or os.getenv("TOTP")   # base32 secret → dynamic OTP
                                                                # 6-digit PIN   → used directly

# ── Constants ───────────────────────────────────────────────────────────────
IST          = pytz.timezone("Asia/Kolkata")
MARKET_OPEN  = (9, 15)
MARKET_CLOSE = (15, 30)
EXCHANGE     = "NSE"
SYMBOL_TOKEN = "99926000"    # Nifty 50 (99926009 = Bank Nifty)
SYMBOL       = "Nifty 50"
LOGS_DIR     = os.path.join(os.path.dirname(__file__), "logs")

# 9-line EMA ribbon (matches Pine Script EMA12–36 ribbon)
RIBBON_PERIODS = [12, 15, 18, 21, 24, 27, 30, 33, 36]

# ── Helpers ─────────────────────────────────────────────────────────────────

def get_totp() -> str:
    """Return a live 6-digit OTP if TOTP_SECRET is a base32 secret (>6 chars),
    otherwise treat it as a static PIN and return it directly."""
    if TOTP_SECRET and len(TOTP_SECRET) > 6:
        return pyotp.TOTP(TOTP_SECRET).now()
    return TOTP_SECRET or ""


def login() -> SmartConnect:
    obj = SmartConnect(api_key=API_KEY)
    obj.timeout = 30          # raise from default 7 s → 30 s
    resp = obj.generateSession(CLIENT_ID, PASSWORD, get_totp())
    if not resp.get("status"):
        raise RuntimeError(f"Login failed: {resp.get('message')}")
    log.info("Logged in as %s", CLIENT_ID)
    return obj


def is_market_open() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:          # Saturday / Sunday
        return False
    oh, om = MARKET_OPEN
    ch, cm = MARKET_CLOSE
    open_dt  = now.replace(hour=oh, minute=om, second=0, microsecond=0)
    close_dt = now.replace(hour=ch, minute=cm, second=0, microsecond=0)
    return open_dt <= now <= close_dt


def _prev_trading_day(d):
    """Return the most recent weekday before `d`."""
    from datetime import timedelta
    d = d - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _api_fetch(obj, from_str, to_str):
    """Single API call with retry. Returns a DataFrame or raises."""
    from requests.exceptions import ReadTimeout, ConnectionError as ReqConnErr
    params = {
        "exchange":    EXCHANGE,
        "symboltoken": SYMBOL_TOKEN,
        "interval":    "ONE_MINUTE",
        "fromdate":    from_str,
        "todate":      to_str,
    }
    last_err = None
    for attempt in range(1, 4):
        try:
            resp = obj.getCandleData(params)
            break
        except (ReadTimeout, ReqConnErr) as e:
            last_err = e
            log.warning("Attempt %d/3 timed out, retrying in 5 s…", attempt)
            time.sleep(5)
    else:
        raise ValueError(f"API unreachable after 3 attempts: {last_err}")

    if not resp.get("status") or not resp.get("data"):
        return pd.DataFrame()

    df = pd.DataFrame(resp["data"], columns=["Date", "Open", "High", "Low", "Close", "Volume"])
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date")
    df = df.apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(IST)
    else:
        df.index = df.index.tz_convert(IST)
    df.index = df.index.tz_localize(None)
    df = df.sort_index()
    return df


# How many extra warmup days to fetch for EMA seeding (5 trading days ≈ 1875 candles)
EMA_WARMUP_DAYS = 5


def fetch_candles(obj: SmartConnect):
    """Return (display_df, full_df) where full_df includes warmup candles for EMA."""
    from datetime import timedelta
    now_ist = datetime.now(IST)
    today   = now_ist.date()

    # Determine the "display day"
    market_open_today = now_ist.replace(
        hour=MARKET_OPEN[0], minute=MARKET_OPEN[1], second=0, microsecond=0
    )
    if now_ist < market_open_today:
        display_date = _prev_trading_day(today)
        log.info("Before market open — showing previous day: %s", display_date)
    else:
        display_date = today

    # Build list of trading days to fetch: warmup days + display day
    days_to_fetch = []
    d = display_date
    for _ in range(EMA_WARMUP_DAYS):
        d = _prev_trading_day(d)
    # Collect all weekdays from d up to display_date
    while d <= display_date:
        if d.weekday() < 5:
            days_to_fetch.append(d)
        d += timedelta(days=1)

    log.info("Fetching %d days: %s → %s", len(days_to_fetch), days_to_fetch[0], days_to_fetch[-1])

    # Fetch day-by-day (AngelOne API resets connection on large multi-day requests)
    frames = []
    for day in days_to_fetch:
        from_str = f"{day} {MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}"
        if day == display_date and day == today and is_market_open():
            to_str = now_ist.strftime("%Y-%m-%d %H:%M")
        else:
            to_str = f"{day} {MARKET_CLOSE[0]:02d}:{MARKET_CLOSE[1]:02d}"
        try:
            day_df = _api_fetch(obj, from_str, to_str)
            if not day_df.empty:
                frames.append(day_df)
                log.info("  %s: %d candles", day, len(day_df))
            else:
                log.warning("  %s: no data (holiday?)", day)
        except Exception as e:
            log.warning("  %s: failed (%s)", day, e)
        time.sleep(0.3)  # small delay to avoid rate limiting

    if not frames:
        raise ValueError("No candle data returned from API")

    full_df = pd.concat(frames).sort_index()
    full_df = full_df[~full_df.index.duplicated(keep='last')]

    # ── Append live candle for the current forming minute ────────────────
    if display_date == today and is_market_open():
        ltp = None
        # Method 1: getMarketData (newer API, more reliable for indices)
        try:
            mkt_resp = obj.getMarketData(mode="LTP", exchangeTokens={EXCHANGE: [SYMBOL_TOKEN]})
            if mkt_resp.get("status") and mkt_resp.get("data"):
                fetched = mkt_resp["data"].get("fetched", [])
                if fetched:
                    ltp = float(fetched[0].get("ltp", 0))
                    log.info("LTP via getMarketData: %.2f", ltp)
        except Exception as e:
            log.warning("getMarketData failed: %s — trying ltpData fallback", e)

        # Method 2: ltpData (fallback)
        if ltp is None or ltp == 0:
            try:
                ltp_resp = obj.ltpData(EXCHANGE, SYMBOL, SYMBOL_TOKEN)
                log.info("ltpData response: %s", ltp_resp)
                if ltp_resp.get("status") and ltp_resp.get("data"):
                    ltp = float(ltp_resp["data"].get("ltp", 0))
                    log.info("LTP via ltpData: %.2f", ltp)
            except Exception as e:
                log.warning("ltpData also failed: %s", e)

        if ltp and ltp > 0:
            # Current minute timestamp (floored to the minute)
            live_ts = now_ist.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            live_ts = pd.Timestamp(live_ts)
            if live_ts in full_df.index:
                # Update existing candle with latest LTP
                full_df.at[live_ts, "High"]  = max(full_df.at[live_ts, "High"], ltp)
                full_df.at[live_ts, "Low"]   = min(full_df.at[live_ts, "Low"], ltp)
                full_df.at[live_ts, "Close"] = ltp
                log.info("Live candle UPDATED — LTP: %.2f @ %s", ltp, live_ts)
            else:
                # Create a new candle for the current minute
                live_candle = pd.DataFrame(
                    {"Open": ltp, "High": ltp, "Low": ltp, "Close": ltp, "Volume": 0},
                    index=pd.DatetimeIndex([live_ts], name="Date"),
                )
                full_df = pd.concat([full_df, live_candle]).sort_index()
                log.info("Live candle CREATED — LTP: %.2f @ %s", ltp, live_ts)
        else:
            log.warning("Could not fetch live LTP from any method")

    # Split into display portion
    display_start = pd.Timestamp(f"{display_date} {MARKET_OPEN[0]:02d}:{MARKET_OPEN[1]:02d}")
    display_df = full_df[full_df.index >= display_start].copy()

    if display_df.empty:
        raise ValueError(f"No candles found for display date {display_date}")

    log.info("Warmup candles: %d, Display candles: %d", len(full_df) - len(display_df), len(display_df))
    return display_df, full_df


def compute_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def save_and_open_chart(display_df: pd.DataFrame, full_df: pd.DataFrame, live: bool = False):
    today_str  = display_df.index[0].strftime("%Y-%m-%d")
    last_close = display_df["Close"].iloc[-1]
    day_high   = display_df["High"].max()
    day_low    = display_df["Low"].min()
    day_range  = day_high - day_low
    candles    = len(display_df)

    # ── Compute EMAs on FULL data (warmup + display), then slice to display ──
    ribbon_full = {}
    for p in RIBBON_PERIODS:
        ribbon_full[p] = compute_ema(full_df["Close"], p)

    # Slice to display range only
    display_idx = display_df.index
    ribbon = {}
    for p in RIBBON_PERIODS:
        ribbon[p] = ribbon_full[p].reindex(display_idx)
    ema12 = ribbon[12]
    ema36 = ribbon[36]

    # Use display_df for everything from here
    df = display_df

    # Build per-bar ribbon color: "bull" | "weakbull" | "bear" | "weakbear" | "neutral"
    def ribbon_color(ema_series):
        colors = []
        for i in range(len(ema_series)):
            if i == 0:
                colors.append("neutral")
                continue
            chg   = ema_series.iloc[i] - ema_series.iloc[i - 1]
            above = ema12.iloc[i] > ema36.iloc[i]
            below = ema12.iloc[i] < ema36.iloc[i]
            if chg >= 0 and above:
                colors.append("bull")
            elif chg < 0 and above:
                colors.append("weakbull")
            elif chg <= 0 and below:
                colors.append("bear")
            elif chg >= 0 and below:
                colors.append("weakbear")
            else:
                colors.append("neutral")
        return colors

    # Prepare candle data as JSON
    candle_data = []
    for idx, row in df.iterrows():
        candle_data.append({
            "time": int(idx.timestamp()),
            "open": round(row["Open"], 2),
            "high": round(row["High"], 2),
            "low": round(row["Low"], 2),
            "close": round(row["Close"], 2),
        })

    volume_data = []
    for idx, row in df.iterrows():
        color = "#22c55e80" if row["Close"] >= row["Open"] else "#ef444480"
        volume_data.append({
            "time": int(idx.timestamp()),
            "value": int(row["Volume"]),
            "color": color,
        })

    # Prepare ribbon EMA data with per-bar colors
    color_map = {
        "bull": "#22c55e", "weakbull": "#3B82F6",
        "bear": "#EF4444", "weakbear": "#FFFFFF", "neutral": "#6B7280"
    }

    # ── Per-bar ribbon state for all 9 EMAs ───────────────────────────────
    all_ribbon_colors = {}  # period → list of color strings
    for p in RIBBON_PERIODS:
        all_ribbon_colors[p] = ribbon_color(ribbon[p])

    bar_count = len(df)
    FAST_EMAS = [12, 15, 18]          # fast group for exit signals
    CHOP_THRESHOLD = 5.0              # min EMA12-EMA36 spread to trade
    RIBBON_MIN_WIDTH = 8.0            # reject overlapping/tight ribbons
    RIBBON_MIN_GAP = 0.8              # each adjacent EMA should be visibly separated
    EDGE_NEAR_FACTOR = 0.35           # price must stay near the active ribbon edge

    # Per-bar counts
    green_count = []   # how many of 9 are "bull"
    red_count   = []   # how many of 9 are "bear"
    fast_green  = []   # how many of fast 3 are "bull"
    fast_red    = []   # how many of fast 3 are "bear"
    ribbon_spread = [] # abs(EMA12 - EMA36)
    ribbon_widths = []
    ribbon_min_gaps = []
    bull_stacks = []
    bear_stacks = []
    near_top_edge = []
    near_bottom_edge = []

    for i in range(bar_count):
        gc = sum(1 for p in RIBBON_PERIODS if all_ribbon_colors[p][i] == "bull")
        rc = sum(1 for p in RIBBON_PERIODS if all_ribbon_colors[p][i] == "bear")
        fg = sum(1 for p in FAST_EMAS if all_ribbon_colors[p][i] == "bull")
        fr = sum(1 for p in FAST_EMAS if all_ribbon_colors[p][i] == "bear")
        ema_values = [ribbon[p].iloc[i] for p in RIBBON_PERIODS]
        adj_gaps = [abs(ema_values[j] - ema_values[j + 1]) for j in range(len(ema_values) - 1)]
        top_edge = max(ema_values)
        bottom_edge = min(ema_values)
        width = top_edge - bottom_edge
        spread = abs(ema12.iloc[i] - ema36.iloc[i])
        min_gap = min(adj_gaps)
        close_price = df["Close"].iloc[i]
        bull_stack = all(ema_values[j] > ema_values[j + 1] for j in range(len(ema_values) - 1))
        bear_stack = all(ema_values[j] < ema_values[j + 1] for j in range(len(ema_values) - 1))
        max_edge_distance = max(2.0, width * EDGE_NEAR_FACTOR)
        green_count.append(gc)
        red_count.append(rc)
        fast_green.append(fg)
        fast_red.append(fr)
        ribbon_spread.append(spread)
        ribbon_widths.append(width)
        ribbon_min_gaps.append(min_gap)
        bull_stacks.append(bull_stack)
        bear_stacks.append(bear_stack)
        near_top_edge.append(close_price >= top_edge and (close_price - top_edge) <= max_edge_distance)
        near_bottom_edge.append(close_price <= bottom_edge and (bottom_edge - close_price) <= max_edge_distance)

    # ── Strategy v1 (original): all 9 green/red ──────────────────────────
    v1_trades = []
    v1_pos = None
    v1_entry_i = None
    v1_entry_price = None

    for i in range(bar_count):
        cp = df["Close"].iloc[i]
        ag = green_count[i] == 9
        ar = red_count[i] == 9
        if v1_pos is None:
            if ag:
                v1_pos, v1_entry_i, v1_entry_price = "LONG", i, cp
            elif ar:
                v1_pos, v1_entry_i, v1_entry_price = "SHORT", i, cp
        elif v1_pos == "LONG" and ar:
            v1_trades.append({"type": "LONG", "entry_i": v1_entry_i, "exit_i": i,
                "entry_price": round(v1_entry_price, 2), "exit_price": round(cp, 2),
                "points": round(cp - v1_entry_price, 2),
                "entry_time": int(df.index[v1_entry_i].timestamp()),
                "exit_time": int(df.index[i].timestamp())})
            v1_pos, v1_entry_i, v1_entry_price = "SHORT", i, cp
        elif v1_pos == "SHORT" and ag:
            v1_trades.append({"type": "SHORT", "entry_i": v1_entry_i, "exit_i": i,
                "entry_price": round(v1_entry_price, 2), "exit_price": round(cp, 2),
                "points": round(v1_entry_price - cp, 2),
                "entry_time": int(df.index[v1_entry_i].timestamp()),
                "exit_time": int(df.index[i].timestamp())})
            v1_pos, v1_entry_i, v1_entry_price = "LONG", i, cp

    if v1_pos and v1_entry_i is not None:
        cp = df["Close"].iloc[-1]
        pts = (cp - v1_entry_price) if v1_pos == "LONG" else (v1_entry_price - cp)
        v1_trades.append({"type": v1_pos, "entry_i": v1_entry_i, "exit_i": bar_count - 1,
            "entry_price": round(v1_entry_price, 2), "exit_price": round(cp, 2),
            "points": round(pts, 2),
            "entry_time": int(df.index[v1_entry_i].timestamp()),
            "exit_time": int(df.index[-1].timestamp()), "open": True})

    # ── Strategy v2 (improved): expanded ribbon + edge pullback entries ──
    ENTRY_THRESHOLD = 6    # keep majority confirmation as a secondary filter
    EXIT_FAST_LOST  = 2    # exit when ≥2 of 3 fast EMAs lose color

    trades = []
    position = None
    entry_i = None
    entry_price = None

    for i in range(bar_count):
        close_price = df["Close"].iloc[i]
        spread = ribbon_spread[i]

        if position is None:
            long_ready = (
                green_count[i] >= ENTRY_THRESHOLD
                and bull_stacks[i]
                and ribbon_widths[i] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i] >= RIBBON_MIN_GAP
                and near_top_edge[i]
                and close_price > ema36.iloc[i]
                and spread >= CHOP_THRESHOLD
            )
            short_ready = (
                red_count[i] >= ENTRY_THRESHOLD
                and bear_stacks[i]
                and ribbon_widths[i] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i] >= RIBBON_MIN_GAP
                and near_bottom_edge[i]
                and close_price < ema36.iloc[i]
                and spread >= CHOP_THRESHOLD
            )

            prev_long_ready = i > 0 and (
                green_count[i - 1] >= ENTRY_THRESHOLD
                and bull_stacks[i - 1]
                and ribbon_widths[i - 1] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i - 1] >= RIBBON_MIN_GAP
                and near_top_edge[i - 1]
            )
            prev_short_ready = i > 0 and (
                red_count[i - 1] >= ENTRY_THRESHOLD
                and bear_stacks[i - 1]
                and ribbon_widths[i - 1] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i - 1] >= RIBBON_MIN_GAP
                and near_bottom_edge[i - 1]
            )

            # ── Fresh LONG signal ──
            if long_ready and not prev_long_ready:
                position = "LONG"
                entry_i = i
                entry_price = close_price

            # ── Fresh SHORT signal ──
            elif short_ready and not prev_short_ready:
                position = "SHORT"
                entry_i = i
                entry_price = close_price

        elif position == "LONG":
            # Exit long: fast EMAs lose green, price slips below EMA12, or stack breaks
            exit_signal = (fast_green[i] <= (len(FAST_EMAS) - EXIT_FAST_LOST)
                           or red_count[i] >= ENTRY_THRESHOLD
                           or close_price < ema12.iloc[i]
                           or not bull_stacks[i])
            if exit_signal:
                pts = close_price - entry_price
                trades.append({
                    "type": "LONG", "entry_i": entry_i, "exit_i": i,
                    "entry_price": round(entry_price, 2),
                    "exit_price": round(close_price, 2),
                    "points": round(pts, 2),
                    "entry_time": int(df.index[entry_i].timestamp()),
                    "exit_time": int(df.index[i].timestamp()),
                })
                position = None

        elif position == "SHORT":
            # Exit short: fast EMAs lose red, price rises above EMA12, or stack breaks
            exit_signal = (fast_red[i] <= (len(FAST_EMAS) - EXIT_FAST_LOST)
                           or green_count[i] >= ENTRY_THRESHOLD
                           or close_price > ema12.iloc[i]
                           or not bear_stacks[i])
            if exit_signal:
                pts = entry_price - close_price
                trades.append({
                    "type": "SHORT", "entry_i": entry_i, "exit_i": i,
                    "entry_price": round(entry_price, 2),
                    "exit_price": round(close_price, 2),
                    "points": round(pts, 2),
                    "entry_time": int(df.index[entry_i].timestamp()),
                    "exit_time": int(df.index[i].timestamp()),
                })
                position = None

    # Close any open V2 position at last candle
    if position and entry_i is not None:
        close_price = df["Close"].iloc[-1]
        pts = (close_price - entry_price) if position == "LONG" else (entry_price - close_price)
        trades.append({
            "type": position, "entry_i": entry_i, "exit_i": bar_count - 1,
            "entry_price": round(entry_price, 2),
            "exit_price": round(close_price, 2),
            "points": round(pts, 2),
            "entry_time": int(df.index[entry_i].timestamp()),
            "exit_time": int(df.index[-1].timestamp()),
            "open": True,
        })

    # ── Strategy v3: same entry as V2, relaxed exit (ride the trend) ─────
    v3_trades = []
    v3_pos = None
    v3_entry_i = None
    v3_entry_price = None

    for i in range(bar_count):
        close_price = df["Close"].iloc[i]
        spread = ribbon_spread[i]

        if v3_pos is None:
            long_ready = (
                green_count[i] >= ENTRY_THRESHOLD
                and bull_stacks[i]
                and ribbon_widths[i] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i] >= RIBBON_MIN_GAP
                and near_top_edge[i]
                and close_price > ema36.iloc[i]
                and spread >= CHOP_THRESHOLD
            )
            short_ready = (
                red_count[i] >= ENTRY_THRESHOLD
                and bear_stacks[i]
                and ribbon_widths[i] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i] >= RIBBON_MIN_GAP
                and near_bottom_edge[i]
                and close_price < ema36.iloc[i]
                and spread >= CHOP_THRESHOLD
            )

            prev_long_ready = i > 0 and (
                green_count[i - 1] >= ENTRY_THRESHOLD
                and bull_stacks[i - 1]
                and ribbon_widths[i - 1] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i - 1] >= RIBBON_MIN_GAP
                and near_top_edge[i - 1]
            )
            prev_short_ready = i > 0 and (
                red_count[i - 1] >= ENTRY_THRESHOLD
                and bear_stacks[i - 1]
                and ribbon_widths[i - 1] >= RIBBON_MIN_WIDTH
                and ribbon_min_gaps[i - 1] >= RIBBON_MIN_GAP
                and near_bottom_edge[i - 1]
            )

            if long_ready and not prev_long_ready:
                v3_pos = "LONG"
                v3_entry_i = i
                v3_entry_price = close_price
            elif short_ready and not prev_short_ready:
                v3_pos = "SHORT"
                v3_entry_i = i
                v3_entry_price = close_price

        elif v3_pos == "LONG":
            # Exit long: only when ribbon majority flips OR price drops below entire ribbon
            exit_signal = (
                green_count[i] < 5             # majority of 9 ribbons lost green
                or close_price < ema36.iloc[i]  # price fell below slowest EMA
            )
            if exit_signal:
                pts = close_price - v3_entry_price
                v3_trades.append({
                    "type": "LONG", "entry_i": v3_entry_i, "exit_i": i,
                    "entry_price": round(v3_entry_price, 2),
                    "exit_price": round(close_price, 2),
                    "points": round(pts, 2),
                    "entry_time": int(df.index[v3_entry_i].timestamp()),
                    "exit_time": int(df.index[i].timestamp()),
                })
                v3_pos = None

        elif v3_pos == "SHORT":
            # Exit short: only when ribbon majority flips OR price rises above entire ribbon
            exit_signal = (
                red_count[i] < 5               # majority of 9 ribbons lost red
                or close_price > ema36.iloc[i]  # price rose above slowest EMA
            )
            if exit_signal:
                pts = v3_entry_price - close_price
                v3_trades.append({
                    "type": "SHORT", "entry_i": v3_entry_i, "exit_i": i,
                    "entry_price": round(v3_entry_price, 2),
                    "exit_price": round(close_price, 2),
                    "points": round(pts, 2),
                    "entry_time": int(df.index[v3_entry_i].timestamp()),
                    "exit_time": int(df.index[i].timestamp()),
                })
                v3_pos = None

    # Close any open V3 position at last candle
    if v3_pos and v3_entry_i is not None:
        close_price = df["Close"].iloc[-1]
        pts = (close_price - v3_entry_price) if v3_pos == "LONG" else (v3_entry_price - close_price)
        v3_trades.append({
            "type": v3_pos, "entry_i": v3_entry_i, "exit_i": bar_count - 1,
            "entry_price": round(v3_entry_price, 2),
            "exit_price": round(close_price, 2),
            "points": round(pts, 2),
            "entry_time": int(df.index[v3_entry_i].timestamp()),
            "exit_time": int(df.index[-1].timestamp()),
            "open": True,
        })

    # ── Stats for both strategies ───────────────────────────────────────────
    def calc_stats(tlist):
        tp = round(sum(t["points"] for t in tlist), 2)
        w  = [t for t in tlist if t["points"] > 0]
        l  = [t for t in tlist if t["points"] < 0]
        wp = round(sum(t["points"] for t in w), 2)
        lp = round(sum(t["points"] for t in l), 2)
        wr = round(len(w) / len(tlist) * 100, 1) if tlist else 0
        return {"total": tp, "winners": len(w), "losers": len(l),
                "win_pts": wp, "loss_pts": lp, "win_rate": wr, "count": len(tlist)}

    v1_stats = calc_stats(v1_trades)
    v2_stats = calc_stats(trades)
    v3_stats = calc_stats(v3_trades)

    # Build markers for V3 trades on chart (V3 = latest strategy, shown on chart)
    markers = []
    for t in v3_trades:
        markers.append({
            "time": t["entry_time"],
            "position": "belowBar" if t["type"] == "LONG" else "aboveBar",
            "color": "#22c55e" if t["type"] == "LONG" else "#ef4444",
            "shape": "arrowUp" if t["type"] == "LONG" else "arrowDown",
            "text": f"{'BUY' if t['type'] == 'LONG' else 'SELL'} @ {t['entry_price']}",
        })
        markers.append({
            "time": t["exit_time"],
            "position": "aboveBar" if t["type"] == "LONG" else "belowBar",
            "color": "#f59e0b",
            "shape": "arrowDown" if t["type"] == "LONG" else "arrowUp",
            "text": f"EXIT @ {t['exit_price']} ({'+' if t['points']>=0 else ''}{t['points']})",
        })
    markers.sort(key=lambda m: m["time"])
    ribbon_series = {}
    for p in RIBBON_PERIODS:
        ema = ribbon[p]
        cols = ribbon_color(ema)
        data = []
        for i, (idx, val) in enumerate(ema.items()):
            data.append({
                "time": int(idx.timestamp()),
                "value": round(val, 2),
                "color": color_map[cols[i]],
            })
        ribbon_series[p] = data

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{SYMBOL} — {today_str} — 1 Min</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0d1117; color: #e6edf3; font-family: 'Segoe UI', sans-serif; }}
  #header {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 24px; background: #161b22; border-bottom: 1px solid #30363d;
  }}
  #header h1 {{ font-size: 18px; font-weight: 600; }}
  #header .stats {{ font-size: 14px; color: #8b949e; }}
  #header .stats span {{ margin-left: 16px; }}
  .ltp {{ color: #f0f6fc; font-weight: 700; font-size: 16px; }}
  .green {{ color: #22c55e; }} .red {{ color: #ef4444; }}
  #chart-container {{ width: 100%; height: calc(100vh - 56px); }}
  #legend {{
    position: absolute; top: 66px; left: 16px; z-index: 10;
    background: rgba(22,27,34,0.9); border: 1px solid #30363d; border-radius: 6px;
    padding: 8px 12px; font-size: 12px; line-height: 1.8;
  }}
  #legend .dot {{ display: inline-block; width: 10px; height: 3px; margin-right: 6px; vertical-align: middle; }}
  #results-link {{
    position: absolute; top: 66px; right: 16px; z-index: 10;
    background: rgba(22,27,34,0.95); border: 1px solid #30363d; border-radius: 8px;
    padding: 10px 18px; font-size: 14px; font-weight: 600; color: #58a6ff;
    text-decoration: none; transition: background 0.2s, border-color 0.2s;
  }}
  #results-link:hover {{ background: #1c2128; border-color: #58a6ff; }}
</style>
</head>
<body>
<div id="header">
  <h1>{SYMBOL} &nbsp;·&nbsp; {today_str} &nbsp;·&nbsp; 1-Min &nbsp;·&nbsp; {candles} candles</h1>
  <div class="stats">
    <span class="ltp">LTP ₹{last_close:,.2f}</span>
    <span class="green">H ₹{day_high:,.2f}</span>
    <span class="red">L ₹{day_low:,.2f}</span>
    <span>Range {day_range:.2f}</span>
  </div>
</div>
<div id="legend">
  <div><span class="dot" style="background:#22c55e"></span>Strong Bullish</div>
  <div><span class="dot" style="background:#3B82F6"></span>Weakening Bull</div>
  <div><span class="dot" style="background:#EF4444"></span>Strong Bearish</div>
  <div><span class="dot" style="background:#FFFFFF"></span>Weakening Bear</div>
</div>
<a id="results-link" href="results.html" target="_blank">📊 Strategy Results</a>
<div id="chart-container"></div>

<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<script>
const container = document.getElementById('chart-container');
const chart = LightweightCharts.createChart(container, {{
  width: container.clientWidth,
  height: container.clientHeight,
  layout: {{
    background: {{ type: 'solid', color: '#0d1117' }},
    textColor: '#8b949e',
    fontSize: 12,
  }},
  grid: {{
    vertLines: {{ color: '#1c2128' }},
    horzLines: {{ color: '#1c2128' }},
  }},
  crosshair: {{
    mode: LightweightCharts.CrosshairMode.Normal,
    vertLine: {{ color: '#555', width: 1, style: 2, labelBackgroundColor: '#2d333b' }},
    horzLine: {{ color: '#555', width: 1, style: 2, labelBackgroundColor: '#2d333b' }},
  }},
  rightPriceScale: {{
    borderColor: '#30363d',
    scaleMargins: {{ top: 0.05, bottom: 0.15 }},
  }},
  timeScale: {{
    borderColor: '#30363d',
    timeVisible: true,
    secondsVisible: false,
    tickMarkFormatter: function(time) {{
      const d = new Date(time * 1000);
      return d.toLocaleTimeString('en-IN', {{ hour: '2-digit', minute: '2-digit', hour12: false, timeZone: 'Asia/Kolkata' }});
    }}
  }},
  handleScroll: {{ mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: true }},
  handleScale: {{ axisPressedMouseMove: true, mouseWheel: true, pinch: true }},
}});

// Candlestick series
const candleSeries = chart.addCandlestickSeries({{
  upColor: '#22c55e', downColor: '#ef4444',
  borderUpColor: '#22c55e', borderDownColor: '#ef4444',
  wickUpColor: '#22c55e', wickDownColor: '#ef4444',
}});
candleSeries.setData({json.dumps(candle_data)});

// Trade markers (BUY/SELL arrows)
candleSeries.setMarkers({json.dumps(markers)});

// Volume as histogram
const volumeSeries = chart.addHistogramSeries({{
  priceFormat: {{ type: 'volume' }},
  priceScaleId: 'volume',
}});
volumeSeries.priceScale().applyOptions({{
  scaleMargins: {{ top: 0.85, bottom: 0 }},
}});
volumeSeries.setData({json.dumps(volume_data)});

// 9-line EMA Ribbon with per-bar coloring
const ribbonConfig = {json.dumps({str(p): ribbon_series[p] for p in RIBBON_PERIODS})};
const lineWidths = {{ '12': 2, '36': 2 }};
for (const [period, data] of Object.entries(ribbonConfig)) {{
  const lw = lineWidths[period] || 1;
  // For colored per-bar lines, we set the base color and use markers + per-point color
  const lineSeries = chart.addLineSeries({{
    color: '#6B7280',
    lineWidth: lw,
    priceLineVisible: false,
    lastValueVisible: false,
    crosshairMarkerVisible: false,
  }});
  // Set data with per-bar color
  const lineData = data.map(d => ({{ time: d.time, value: d.value, color: d.color }}));
  lineSeries.setData(lineData);
}}

// Responsive resize
window.addEventListener('resize', () => {{
  chart.applyOptions({{ width: container.clientWidth, height: container.clientHeight }});
}});

// Fit content
chart.timeScale().fitContent();
</script>
</body>
</html>"""

    # Save HTML
    day_dir = os.path.join(LOGS_DIR, today_str)
    os.makedirs(day_dir, exist_ok=True)
    ts   = datetime.now(IST).strftime("%H%M%S")
    html_path = os.path.join(day_dir, f"nifty_{today_str}_{ts}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info("Chart saved → %s", html_path)

    # Also always write a latest.html for quick access
    latest_path = os.path.join(os.path.dirname(__file__), "chart.html")
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(html)

    # ── Generate results.html (separate page) ───────────────────────────
    results_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Strategy Results — {SYMBOL} — {today_str}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0d1117; color: #e6edf3; font-family: 'Segoe UI', sans-serif; padding: 24px; }}
  a {{ color: #58a6ff; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .back {{ display: inline-block; margin-bottom: 20px; font-size: 14px; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .subtitle {{ color: #8b949e; font-size: 14px; margin-bottom: 24px; }}
  .compare {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-bottom: 28px; max-width: 900px; }}
  .compare-box {{ background:#161b22; border:1px solid #30363d; border-radius:8px; padding:14px 18px; }}
  .compare-box.active {{ border-color:#22c55e55; }}
  .strat-label {{ font-size: 11px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }}
  .net {{ font-size: 26px; font-weight: 700; }}
  .net-pos {{ color: #22c55e; }} .net-neg {{ color: #ef4444; }}
  .meta {{ font-size: 12px; color: #8b949e; margin-top: 2px; }}
  .section {{ max-width: 700px; margin-bottom: 32px; }}
  h2 {{ font-size: 17px; border-bottom: 1px solid #30363d; padding-bottom: 6px; margin-bottom: 12px; }}
  .summary {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px 16px; margin-bottom: 14px; }}
  .summary .label {{ color: #8b949e; font-size: 12px; }}
  .summary .val {{ font-weight: 600; font-size: 14px; }}
  .green {{ color: #22c55e; }} .red {{ color: #ef4444; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ color: #8b949e; text-align: left; padding: 6px 8px; border-bottom: 1px solid #30363d; }}
  td {{ padding: 6px 8px; border-bottom: 1px solid #1c2128; }}
  .pts-pos {{ color: #22c55e; font-weight: 600; }}
  .pts-neg {{ color: #ef4444; font-weight: 600; }}
  .rules {{ background:#161b22; border:1px solid #22c55e33; border-radius:8px; padding:14px 18px; margin-top:20px; font-size:13px; line-height:1.8; color:#8b949e; max-width: 700px; }}
  .rules b {{ color:#f0f6fc; }}
  .note {{ color:#8b949e; font-size:11px; margin-top:12px; }}
</style>
</head>
<body>
<a class="back" href="chart.html">← Back to Chart</a>
<h1>{SYMBOL} — Strategy Results</h1>
<div class="subtitle">{today_str} · {candles} candles · LTP ₹{last_close:,.2f}</div>

<div class="compare">
  <div class="compare-box">
    <div class="strat-label">V1 · All-9</div>
    <div class="net {'net-pos' if v1_stats['total'] >= 0 else 'net-neg'}">{'+' if v1_stats['total'] >= 0 else ''}{v1_stats['total']} pts</div>
    <div class="meta">{v1_stats['count']} trades · {v1_stats['win_rate']}% win</div>
  </div>
  <div class="compare-box">
    <div class="strat-label">V2 · Ribbon Edge</div>
    <div class="net {'net-pos' if v2_stats['total'] >= 0 else 'net-neg'}">{'+' if v2_stats['total'] >= 0 else ''}{v2_stats['total']} pts</div>
    <div class="meta">{v2_stats['count']} trades · {v2_stats['win_rate']}% win</div>
  </div>
  <div class="compare-box active">
    <div class="strat-label">V3 · Trend Ride ✦</div>
    <div class="net {'net-pos' if v3_stats['total'] >= 0 else 'net-neg'}">{'+' if v3_stats['total'] >= 0 else ''}{v3_stats['total']} pts</div>
    <div class="meta">{v3_stats['count']} trades · {v3_stats['win_rate']}% win</div>
  </div>
</div>

<div class="section">
  <h2>V3 — Trend Ride (arrows on chart) ✦</h2>
  <div class="summary">
    <div><span class="label">Trades</span><br><span class="val">{v3_stats['count']}</span></div>
    <div><span class="label">Winners</span><br><span class="val green">{v3_stats['winners']}</span></div>
    <div><span class="label">Losers</span><br><span class="val red">{v3_stats['losers']}</span></div>
    <div><span class="label">Win Pts</span><br><span class="val green">+{v3_stats['win_pts']}</span></div>
    <div><span class="label">Loss Pts</span><br><span class="val red">{v3_stats['loss_pts']}</span></div>
    <div><span class="label">Win Rate</span><br><span class="val">{v3_stats['win_rate']}%</span></div>
  </div>
  <table>
    <tr><th>#</th><th>Type</th><th>Entry</th><th>Exit</th><th>Pts</th></tr>
    {''.join(
        f'<tr><td>{i+1}</td><td>{"🟢 LONG" if t["type"]=="LONG" else "🔴 SHORT"}</td>'
        f'<td>₹{t["entry_price"]}</td><td>₹{t["exit_price"]}{"*" if t.get("open") else ""}</td>'
        f'<td class="{"pts-pos" if t["points"]>=0 else "pts-neg"}">'
        f'{("+" if t["points"]>=0 else "")}{t["points"]}</td></tr>'
        for i, t in enumerate(v3_trades)
    )}
  </table>
</div>

<div class="section">
  <h2>V2 — Ribbon Edge (fast exit)</h2>
  <div class="summary">
    <div><span class="label">Trades</span><br><span class="val">{v2_stats['count']}</span></div>
    <div><span class="label">Winners</span><br><span class="val green">{v2_stats['winners']}</span></div>
    <div><span class="label">Losers</span><br><span class="val red">{v2_stats['losers']}</span></div>
    <div><span class="label">Win Pts</span><br><span class="val green">+{v2_stats['win_pts']}</span></div>
    <div><span class="label">Loss Pts</span><br><span class="val red">{v2_stats['loss_pts']}</span></div>
    <div><span class="label">Win Rate</span><br><span class="val">{v2_stats['win_rate']}%</span></div>
  </div>
  <table>
    <tr><th>#</th><th>Type</th><th>Entry</th><th>Exit</th><th>Pts</th></tr>
    {''.join(
        f'<tr><td>{i+1}</td><td>{"🟢 LONG" if t["type"]=="LONG" else "🔴 SHORT"}</td>'
        f'<td>₹{t["entry_price"]}</td><td>₹{t["exit_price"]}{"*" if t.get("open") else ""}</td>'
        f'<td class="{"pts-pos" if t["points"]>=0 else "pts-neg"}">'
        f'{("+" if t["points"]>=0 else "")}{t["points"]}</td></tr>'
        for i, t in enumerate(trades)
    )}
  </table>
</div>

<div class="section">
  <h2>V1 — Original (All-9 Green/Red)</h2>
  <div class="summary">
    <div><span class="label">Trades</span><br><span class="val">{v1_stats['count']}</span></div>
    <div><span class="label">Winners</span><br><span class="val green">{v1_stats['winners']}</span></div>
    <div><span class="label">Losers</span><br><span class="val red">{v1_stats['losers']}</span></div>
    <div><span class="label">Win Pts</span><br><span class="val green">+{v1_stats['win_pts']}</span></div>
    <div><span class="label">Loss Pts</span><br><span class="val red">{v1_stats['loss_pts']}</span></div>
    <div><span class="label">Win Rate</span><br><span class="val">{v1_stats['win_rate']}%</span></div>
  </div>
  <table>
    <tr><th>#</th><th>Type</th><th>Entry</th><th>Exit</th><th>Pts</th></tr>
    {''.join(
        f'<tr><td>{i+1}</td><td>{"🟢 LONG" if t["type"]=="LONG" else "🔴 SHORT"}</td>'
        f'<td>₹{t["entry_price"]}</td><td>₹{t["exit_price"]}{"*" if t.get("open") else ""}</td>'
        f'<td class="{"pts-pos" if t["points"]>=0 else "pts-neg"}">'
        f'{("+" if t["points"]>=0 else "")}{t["points"]}</td></tr>'
        for i, t in enumerate(v1_trades)
    )}
  </table>
</div>

<div class="rules">
  <b>V2 Rules (Ribbon Edge — fast exit):</b><br>
  ENTRY: Same as V3 below<br>
  EXIT: Fast EMAs lose direction, price slips below EMA12, or ribbon stack breaks<br>
  <b>Goes flat after exit — no forced reversals</b>
</div>
<div class="rules" style="margin-top:12px;border-color:#a855f755">
  <b>V3 Rules (Trend Ride — relaxed exit):</b><br>
  ENTRY: Ribbon must be cleanly stacked and expanded<br>
  &nbsp;&nbsp;+ ≥6/9 ribbons agree with direction<br>
  &nbsp;&nbsp;+ Adjacent EMA gaps stay separated, not overlapped<br>
  &nbsp;&nbsp;+ Price must hug the active edge: top green edge for LONG, bottom red edge for SHORT<br>
  &nbsp;&nbsp;+ Close stays above/below EMA36 and EMA12-EMA36 spread stays wide enough<br>
  EXIT: Only when ribbon majority flips (&lt;5/9 green for LONG, &lt;5/9 red for SHORT)<br>
  &nbsp;&nbsp;OR price drops below EMA36 (LONG) / rises above EMA36 (SHORT)<br>
  <b>Goes flat after exit — stays in trade as long as ribbon holds</b>
</div>
<div class="note">* = still open at last candle</div>

</body>
</html>"""

    results_path = os.path.join(os.path.dirname(__file__), "results.html")
    with open(results_path, "w", encoding="utf-8") as f:
        f.write(results_html)
    log.info("Results page saved → %s", results_path)

    # Open in browser
    webbrowser.open(f"file:///{latest_path.replace(os.sep, '/')}")
    log.info("Chart opened in browser")


# ── Modes ───────────────────────────────────────────────────────────────────

def run_once():
    """Fetch and plot today's data once."""
    obj = login()
    display_df, full_df = fetch_candles(obj)
    log.info(
        "Candles: %d  |  LTP: %.2f  |  H: %.2f  |  L: %.2f",
        len(display_df), display_df["Close"].iloc[-1], display_df["High"].max(), display_df["Low"].min(),
    )
    save_and_open_chart(display_df, full_df, live=False)


def run_live(refresh_seconds: int = 60):
    """Auto-refresh chart every `refresh_seconds` while market is open."""
    obj = login()
    log.info("Live mode — refresh every %ds. Ctrl+C to stop.", refresh_seconds)
    try:
        while True:
            try:
                display_df, full_df = fetch_candles(obj)
                log.info(
                    "Refreshed  |  %d candles  |  LTP: %.2f  |  H: %.2f  |  L: %.2f",
                    len(display_df), display_df["Close"].iloc[-1], display_df["High"].max(), display_df["Low"].min(),
                )
                open_now = is_market_open()
                save_and_open_chart(display_df, full_df, live=open_now)
                if not open_now:
                    log.info("Market closed. Final chart saved.")
                    break
            except Exception as e:
                log.error("Error: %s", e)
            time.sleep(refresh_seconds)
    except KeyboardInterrupt:
        log.info("Stopped by user.")


# ── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode    = sys.argv[1] if len(sys.argv) > 1 else "once"
    if mode == "live":
        refresh = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        run_live(refresh_seconds=refresh)
    else:
        run_once()
