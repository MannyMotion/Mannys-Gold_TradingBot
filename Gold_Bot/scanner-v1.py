"""
scanner.py — Manny's Gold Strategy V3 Mixed Bias Edition
=========================================================
Purpose:
    Connects to MetaTrader 5 (MT5) and scans multiple symbols across multiple
    timeframes for high-probability Smart Money Concepts (SMC) trade signals.
    Signals are filtered by bias alignment, structure, order blocks, FVGs,
    and a confidence score gate before auto-executing or alerting manually.

Inputs:
    - MT5 demo/live account credentials (via .env file)
    - Telegram bot token and chat ID (via .env file)
    - Live OHLCV data pulled from MT5 via MetaTrader5 Python API

Outputs:
    - Auto-executed trades on MT5 for signals scoring >= session threshold
    - Telegram alerts for manual review on signals scoring 5-6
    - trade_log.csv: full audit trail of every signal fired

Updates in this version (V3 Optimized):
    ✅ PRIORITY 1: Confidence score now gates signal execution (7+ auto, 5-6 manual, <5 ignored)
    ✅ PRIORITY 2: Daily bias fallback removed — always uses midnight UTC candle
    ✅ PRIORITY 3: BOS reset is now timeframe-aware (50/30/20 bars depending on TF)
    ✅ PRIORITY 4: OB candle index corrected from iloc[-6] to iloc[-5]
    ✅ PRIORITY 5: CSV trade logging system added (trade_log.csv auto-created)
    ✅ PRIORITY 6: Session-based confidence thresholds (Post-NY requires score 8, others 7)

Author: Emmanuel Ogbu (Manny)
Date: April 2026
"""

import csv                          # built-in Python module for reading/writing CSV files
import os                           # built-in module for file system checks
import time                         # built-in module for sleep between scan loops
import requests                     # third-party: HTTP requests (for Telegram API)
import MetaTrader5 as mt5           # MT5 Python API for live trading connection
import pandas as pd                 # data manipulation and OHLCV DataFrame handling
import numpy as np                  # numerical operations (not used directly but kept for compatibility)
from dotenv import load_dotenv      # loads environment variables from .env file
from datetime import datetime, timezone, timedelta  # timezone-aware datetime handling

load_dotenv()  # load credentials from .env file into environment variables

# ═══════════════════════════════════════════
# CREDENTIALS — loaded from .env file
# Never hardcode credentials directly in code
# ═══════════════════════════════════════════
MT5_LOGIN        = int(os.getenv("MT5_LOGIN"))       # MT5 account number
MT5_PASSWORD     = os.getenv("MT5_PASSWORD")          # MT5 account password
MT5_SERVER       = os.getenv("MT5_SERVER")            # broker server name
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")        # Telegram bot token
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")      # Telegram chat ID to send alerts

# ═══════════════════════════════════════════
# SYMBOLS TO SCAN
# ═══════════════════════════════════════════
SYMBOLS = ["XAUUSD", "XAGUSD", "US500", "US30", "BTCUSD", "QQQ.NAS", "NZDUSD"]

# 3AM pairs use different bias candle hours to Gold
THREE_AM_PAIRS = {"XAGUSD", "US500", "US30", "USOIL"}

# ═══════════════════════════════════════════
# TIMEFRAMES PER SYMBOL
# ═══════════════════════════════════════════
SYMBOL_TIMEFRAMES = {
    "XAUUSD":  [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "XAGUSD":  [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "US500":   [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "US30":    [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "BTCUSD":  [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "QQQ.NAS": [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
    "NZDUSD":  [mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30, mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H4],
}

# Human-readable names for each MT5 timeframe constant
TIMEFRAME_NAMES = {
    mt5.TIMEFRAME_M5:  "5M",
    mt5.TIMEFRAME_M15: "15M",
    mt5.TIMEFRAME_M30: "30M",
    mt5.TIMEFRAME_H1:  "1H",
    mt5.TIMEFRAME_H4:  "4H",
}

# ═══════════════════════════════════════════
# GLOBAL SETTINGS
# ═══════════════════════════════════════════
SYMBOL         = "XAUUSD"             # default symbol (used when no symbol passed explicitly)
TIMEFRAME_4H   = mt5.TIMEFRAME_H4    # 4-hour timeframe constant for bias detection
TIMEFRAME_1H   = mt5.TIMEFRAME_H1    # 1-hour timeframe constant for HTF EMA
RISK_PERCENT   = 0.01                 # 1% of account balance risked per trade
RR             = 3.0                  # reward-to-risk ratio (1:3)
CHECK_INTERVAL = 60                   # seconds between each scan loop
ADX_PERIOD     = 14                   # ADX indicator period
ADX_THRESH     = 25                   # ADX threshold — above this = trending market
ATR_PERIOD     = 14                   # ATR indicator period
SCALP_MIN_RR   = 1.5                  # minimum reward:risk for scalp signals

# ═══════════════════════════════════════════
# SESSION HOURS (UTC)
# ═══════════════════════════════════════════
LONDON_START  = 7
LONDON_END    = 12
OVERLAP_START = 12
OVERLAP_END   = 13
NY_START      = 13
NY_END        = 17
POSTNY_START  = 17
POSTNY_END    = 22

# ═══════════════════════════════════════════
# PRIORITY 1 — CONFIDENCE SCORE THRESHOLDS
# ─────────────────────────────────────────────
# Signals are gated by confidence score (0-10).
# Auto-execute: score >= session threshold (7 for prime, 8 for Post-NY)
# Manual alert: score 5-6 (Telegram alert only, no auto trade)
# Ignore: score < 5 (too weak, not worth alerting)
# Session thresholds defined below. Post-NY is stricter due to thin liquidity.
# ═══════════════════════════════════════════
CONFIDENCE_THRESHOLD_PRIME  = 7   # London, Overlap, New York — standard threshold
CONFIDENCE_THRESHOLD_POSTNY = 8   # Post-NY — higher bar due to chop and thin liquidity
CONFIDENCE_MANUAL_MIN       = 5   # minimum score for manual alert (below this = ignored)

# ═══════════════════════════════════════════
# PRIORITY 5 — TRADE LOG FILE
# Auto-created on first trade. No manual setup needed.
# ═══════════════════════════════════════════
TRADE_LOG_FILE = "trade_log.csv"

# CSV column headers for the trade log
TRADE_LOG_HEADERS = [
    "timestamp",       # UTC time of signal
    "symbol",          # e.g. XAUUSD
    "timeframe",       # e.g. 30M
    "tier",            # STRONG / MEDIUM / SCALP
    "confidence_score",# 0-10 score at time of signal
    "entry",           # entry price
    "sl",              # stop loss price
    "tp",              # take profit price
    "trigger_type",    # OB / FVG / Sweep / Pin / PDH / CHoCH
    "sl_source",       # OB / FVG / Swing / ATR
    "session",         # London / Overlap / New York / Post-NY
    "status",          # auto-executed / manual-alert / ignored
]

# ═══════════════════════════════════════════
# BIAS CANDLE HOURS (UTC)
# Both candles are same for all pairs and all seasons.
# DST shifts display labels only, not actual candle detection.
# ═══════════════════════════════════════════
BIAS_CANDLE1_HOUR = 0   # 00:00 UTC — candle 1 for all pairs, all seasons
BIAS_CANDLE2_HOUR = 4   # 04:00 UTC — candle 2 for all pairs, all seasons

# ═══════════════════════════════════════════
# NEWS BLACKOUT DATES
# ±30 minutes around NFP, CPI, FOMC — no trading allowed
# Matches Pine Script cpiDates and fomcDates arrays exactly
# ═══════════════════════════════════════════
CPI_DATES = [
    datetime(2024,1,11,13,30,tzinfo=timezone.utc), datetime(2024,2,13,13,30,tzinfo=timezone.utc),
    datetime(2024,3,12,13,30,tzinfo=timezone.utc), datetime(2024,4,10,13,30,tzinfo=timezone.utc),
    datetime(2024,5,15,13,30,tzinfo=timezone.utc), datetime(2024,6,12,13,30,tzinfo=timezone.utc),
    datetime(2024,7,11,13,30,tzinfo=timezone.utc), datetime(2024,8,14,13,30,tzinfo=timezone.utc),
    datetime(2024,9,11,13,30,tzinfo=timezone.utc), datetime(2024,10,10,13,30,tzinfo=timezone.utc),
    datetime(2024,11,13,13,30,tzinfo=timezone.utc),datetime(2024,12,11,13,30,tzinfo=timezone.utc),
    datetime(2025,1,15,13,30,tzinfo=timezone.utc), datetime(2025,2,12,13,30,tzinfo=timezone.utc),
    datetime(2025,3,12,13,30,tzinfo=timezone.utc), datetime(2025,4,9,13,30,tzinfo=timezone.utc),
    datetime(2025,5,13,13,30,tzinfo=timezone.utc), datetime(2025,6,11,13,30,tzinfo=timezone.utc),
    datetime(2025,7,11,13,30,tzinfo=timezone.utc), datetime(2025,8,12,13,30,tzinfo=timezone.utc),
    datetime(2025,9,10,13,30,tzinfo=timezone.utc), datetime(2025,10,9,13,30,tzinfo=timezone.utc),
    datetime(2025,11,12,13,30,tzinfo=timezone.utc),datetime(2025,12,10,13,30,tzinfo=timezone.utc),
    datetime(2026,1,14,13,30,tzinfo=timezone.utc), datetime(2026,2,12,13,30,tzinfo=timezone.utc),
    datetime(2026,3,11,13,30,tzinfo=timezone.utc), datetime(2026,4,10,13,30,tzinfo=timezone.utc),
    datetime(2026,5,13,13,30,tzinfo=timezone.utc), datetime(2026,6,11,13,30,tzinfo=timezone.utc),
    datetime(2026,7,14,13,30,tzinfo=timezone.utc), datetime(2026,8,12,13,30,tzinfo=timezone.utc),
    datetime(2026,9,10,13,30,tzinfo=timezone.utc), datetime(2026,10,8,13,30,tzinfo=timezone.utc),
    datetime(2026,11,12,13,30,tzinfo=timezone.utc),datetime(2026,12,9,13,30,tzinfo=timezone.utc),
]

FOMC_DATES = [
    datetime(2024,1,31,19,0,tzinfo=timezone.utc),  datetime(2024,3,20,19,0,tzinfo=timezone.utc),
    datetime(2024,5,1,19,0,tzinfo=timezone.utc),   datetime(2024,6,12,19,0,tzinfo=timezone.utc),
    datetime(2024,7,31,19,0,tzinfo=timezone.utc),  datetime(2024,9,18,19,0,tzinfo=timezone.utc),
    datetime(2024,11,7,19,0,tzinfo=timezone.utc),  datetime(2024,12,18,19,0,tzinfo=timezone.utc),
    datetime(2025,1,29,19,0,tzinfo=timezone.utc),  datetime(2025,3,19,19,0,tzinfo=timezone.utc),
    datetime(2025,5,7,19,0,tzinfo=timezone.utc),   datetime(2025,6,18,19,0,tzinfo=timezone.utc),
    datetime(2025,7,30,19,0,tzinfo=timezone.utc),  datetime(2025,9,17,19,0,tzinfo=timezone.utc),
    datetime(2025,11,5,19,0,tzinfo=timezone.utc),  datetime(2025,12,17,19,0,tzinfo=timezone.utc),
    datetime(2026,1,28,19,0,tzinfo=timezone.utc),  datetime(2026,3,18,19,0,tzinfo=timezone.utc),
    datetime(2026,5,6,19,0,tzinfo=timezone.utc),   datetime(2026,6,17,19,0,tzinfo=timezone.utc),
    datetime(2026,7,29,19,0,tzinfo=timezone.utc),  datetime(2026,9,16,19,0,tzinfo=timezone.utc),
    datetime(2026,11,4,19,0,tzinfo=timezone.utc),  datetime(2026,12,16,19,0,tzinfo=timezone.utc),
]

NEWS_WINDOW = timedelta(minutes=30)  # block trading 30 mins before AND after news event


# ═══════════════════════════════════════════
# PRIORITY 5 — TRADE LOGGING FUNCTION
# ─────────────────────────────────────────────
# Called every time a signal fires — before placing trade or sending Telegram.
# Appends a row to trade_log.csv. Auto-creates file with headers on first call.
# Trade log enables post-session analysis of win/loss patterns by timeframe,
# session, and confidence tier — you can't improve what you don't measure.
# ═══════════════════════════════════════════
def log_trade(
    symbol: str,
    timeframe: str,
    tier: str,
    confidence_score: int,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    trigger_type: str,
    sl_source: str,
    session: str,
    status: str,
) -> None:
    """
    Append a trade signal record to trade_log.csv.

    Args:
        symbol:           Trading symbol e.g. 'XAUUSD'
        timeframe:        Timeframe string e.g. '30M'
        tier:             Signal tier — 'STRONG', 'MEDIUM', or 'SCALP'
        confidence_score: Integer 0-10 representing confluence strength
        entry_price:      Price at signal fire time
        sl_price:         Stop loss price
        tp_price:         Take profit price
        trigger_type:     What triggered the signal e.g. 'OB', 'FVG', 'Sweep'
        sl_source:        Where SL was anchored e.g. 'OB', 'FVG', 'Swing', 'ATR'
        session:          Active session name e.g. 'London', 'Post-NY'
        status:           'auto-executed', 'manual-alert', or 'ignored'

    Returns:
        None. Writes directly to trade_log.csv.
    """
    # Check if the CSV file already exists — if not, we need to write headers first
    file_exists = os.path.isfile(TRADE_LOG_FILE)

    # Open in append mode ('a') so we never overwrite existing records
    # newline='' is required on Windows to prevent double line breaks in CSV
    with open(TRADE_LOG_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_LOG_HEADERS)

        # Write the header row only on the very first trade (file didn't exist before)
        if not file_exists:
            writer.writeheader()
            print(f"📋 trade_log.csv created — audit trail started")

        # Write the trade row with all relevant data
        writer.writerow({
            "timestamp":        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            "symbol":           symbol,
            "timeframe":        timeframe,
            "tier":             tier,
            "confidence_score": confidence_score,
            "entry":            round(entry_price, 5),
            "sl":               round(sl_price, 5),
            "tp":               round(tp_price, 5),
            "trigger_type":     trigger_type,
            "sl_source":        sl_source,
            "session":          session,
            "status":           status,
        })


# ═══════════════════════════════════════════
# PRIORITY 1 — CONFIDENCE SCORE CALCULATION
# ─────────────────────────────────────────────
# Mirrors the Pine Script confScore variable (0-10 scale).
# In Pine Script, confScore is calculated and DISPLAYED but never gates trades.
# Here in Python, we calculate it AND use it as a hard gate.
#
# Scoring breakdown (max 10):
#   +1  weekly bias confirmed
#   +1  daily bias confirmed
#   +2  bias alignment strong (weekly + daily same direction)
#   +1  HTF EMA confirmed
#   +1  active session (not Asian)
#   +1  not extreme bear
#   +1  structure score meets minimum
#   +1  trigger active
#   +1  bonus score meets minimum
# Total possible: 10
# ═══════════════════════════════════════════
def calc_confidence_score(
    wk_bull: bool,
    wk_bear: bool,
    day_bull: bool,
    day_bear: bool,
    htf_bull: bool,
    htf_bear: bool,
    active_session: bool,
    ext_bear: bool,
    s2_long: int,
    s2_short: int,
    any_trig_bull: bool,
    any_trig_bear: bool,
    bon_l: int,
    bon_s: int,
    min_str2: int,
    min_bonus: int,
    is_bull_signal: bool,
) -> int:
    """
    Calculate confidence score (0-10) for the current signal direction.

    Args:
        wk_bull:        Weekly bias is bullish
        wk_bear:        Weekly bias is bearish
        day_bull:       Daily bias is bullish
        day_bear:       Daily bias is bearish
        htf_bull:       Higher timeframe EMA confirms bullish
        htf_bear:       Higher timeframe EMA confirms bearish
        active_session: Currently in a tradeable session (not Asian)
        ext_bear:       Extreme bear market detected (price > 20% below EMA200)
        s2_long:        Tier 2 structure score for longs (0-5)
        s2_short:       Tier 2 structure score for shorts (0-5)
        any_trig_bull:  At least one Tier 3 bull trigger is active
        any_trig_bear:  At least one Tier 3 bear trigger is active
        bon_l:          Tier 4 bonus score for longs (0-7)
        bon_s:          Tier 4 bonus score for shorts (0-7)
        min_str2:       Minimum structure score required
        min_bonus:      Minimum bonus score required
        is_bull_signal: True if evaluating a bull signal, False for bear

    Returns:
        Integer confidence score from 0 to 10.
    """
    score = 0  # start from zero and add points for each confirmed element

    # +1 for weekly bias being confirmed in either direction
    if wk_bull or wk_bear:
        score += 1

    # +1 for daily bias being confirmed in either direction
    if day_bull or day_bear:
        score += 1

    # +2 for STRONG bias alignment (weekly and daily agree on same direction)
    # +1 for MEDIUM alignment (weekly and daily disagree — counter-trend trade)
    bias_strong = (wk_bull and day_bull) or (wk_bear and day_bear)
    bias_medium = (wk_bull and day_bear) or (wk_bear and day_bull)
    if bias_strong:
        score += 2   # both timeframes agree — full points
    elif bias_medium:
        score += 1   # counter-trend setup — partial points

    # +1 for HTF EMA confirming the signal direction
    if (is_bull_signal and htf_bull) or (not is_bull_signal and htf_bear):
        score += 1

    # +1 for being in an active trading session (not Asian)
    if active_session:
        score += 1

    # +1 for NOT being in extreme bear conditions
    if not ext_bear:
        score += 1

    # +1 for structure score meeting minimum threshold
    s2 = s2_long if is_bull_signal else s2_short
    if s2 >= min_str2:
        score += 1

    # +1 for having an active Tier 3 trigger
    trig_active = any_trig_bull if is_bull_signal else any_trig_bear
    if trig_active:
        score += 1

    # +1 for bonus score (RSI div, MACD, etc.) meeting minimum
    bon = bon_l if is_bull_signal else bon_s
    if bon >= min_bonus:
        score += 1

    return score  # final score out of 10


# ═══════════════════════════════════════════
# PRIORITY 6 — SESSION-BASED CONFIDENCE THRESHOLD
# ─────────────────────────────────────────────
# Different sessions have different reliability levels.
# Post-NY is thin and choppy — we require higher conviction.
# Prime sessions (London, Overlap, NY) keep the standard threshold.
# ═══════════════════════════════════════════
def get_session_threshold(session: str) -> int:
    """
    Return the confidence score threshold required for auto-execution
    based on the current trading session.

    Post-NY requires a higher score (8) because liquidity is thinner
    and price action is choppier — marginal setups fail more often.
    All prime sessions use the standard threshold (7).

    Args:
        session: Session name string e.g. 'London', 'Post-NY'

    Returns:
        Integer threshold (7 or 8)
    """
    # Post-NY requires higher confidence due to thin liquidity and choppy price action
    if session == "Post-NY":
        return CONFIDENCE_THRESHOLD_POSTNY   # 8

    # London, Overlap, New York — standard threshold
    return CONFIDENCE_THRESHOLD_PRIME        # 7


# ═══════════════════════════════════════════
# DST AUTO-DETECTION
# ─────────────────────────────────────────────
# BST: last Sunday of March 01:00 UTC → last Sunday of October 01:00 UTC
# ═══════════════════════════════════════════
def is_bst() -> bool:
    """
    Detect whether UK is currently on British Summer Time (BST).

    Returns:
        True if currently BST, False if GMT.
    """
    now  = datetime.now(timezone.utc)
    year = now.year

    # Last Sunday of March at 01:00 UTC — BST starts
    march_end = datetime(year, 4, 1, tzinfo=timezone.utc)
    bst_start = march_end - timedelta(days=(march_end.weekday() + 1) % 7)
    bst_start = bst_start.replace(hour=1, minute=0, second=0, microsecond=0)

    # Last Sunday of October at 01:00 UTC — BST ends
    oct_end = datetime(year, 11, 1, tzinfo=timezone.utc)
    bst_end = oct_end - timedelta(days=(oct_end.weekday() + 1) % 7)
    bst_end = bst_end.replace(hour=1, minute=0, second=0, microsecond=0)

    return bst_start <= now < bst_end


def get_dst_str() -> str:
    """Return 'BST' if summer time, else 'GMT'."""
    return "BST" if is_bst() else "GMT"


def get_pair_bias_hours(symbol: str) -> tuple:
    """
    Returns (h1_utc, h2_utc) — the target bias display times for a symbol.
    Used for Telegram labels only. Actual candle detection uses 00:00 and 04:00 UTC.

    Args:
        symbol: Trading symbol string e.g. 'XAUUSD'

    Returns:
        Tuple of two integers representing UTC hours for display.
    """
    summer = is_bst()
    if symbol in THREE_AM_PAIRS:
        return (2, 6)   # 3AM pairs — same UTC hours regardless of DST
    return (1, 5)       # Gold — 1AM + 5AM UTC regardless of DST


# ═══════════════════════════════════════════
# NEWS BLACKOUT
# ═══════════════════════════════════════════
def is_news_blackout() -> tuple:
    """
    Check whether current time is within ±30 minutes of NFP, CPI, or FOMC.
    Matches Pine Script newsBlackout logic exactly.

    Returns:
        Tuple of (bool, str) — (is_blackout, reason_string)
    """
    now = datetime.now(timezone.utc)

    # NFP — first Friday of the month at 13:30 UTC
    # weekday() == 4 means Friday in Python (Monday = 0)
    if now.weekday() == 4 and now.day <= 7:
        nfp_today = now.replace(hour=13, minute=30, second=0, microsecond=0)
        if abs(now - nfp_today) <= NEWS_WINDOW:
            return True, "NFP ±30min"

    # CPI dates — check against known list
    for dt in CPI_DATES:
        if abs(now - dt) <= NEWS_WINDOW:
            return True, "CPI ±30min"

    # FOMC dates — check against known list
    for dt in FOMC_DATES:
        if abs(now - dt) <= NEWS_WINDOW:
            return True, "FOMC ±30min"

    return False, "Clear"


# ═══════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════
def send_telegram(message: str) -> None:
    """
    Send a message to the configured Telegram chat via bot API.

    Args:
        message: HTML-formatted string to send.
    """
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Telegram error: {e}")


# ═══════════════════════════════════════════
# SESSION DETECTION
# ═══════════════════════════════════════════
def get_session() -> str:
    """
    Determine the current trading session based on UTC hour.

    Returns:
        Session name string: 'London', 'Overlap', 'New York', 'Post-NY', or 'Asian'
    """
    h = datetime.now(timezone.utc).hour
    if   LONDON_START  <= h < LONDON_END:  return "London"
    elif OVERLAP_START <= h < OVERLAP_END: return "Overlap"
    elif NY_START      <= h < NY_END:      return "New York"
    elif POSTNY_START  <= h < POSTNY_END:  return "Post-NY"
    else:                                  return "Asian"


def is_active_session() -> bool:
    """Return True if currently in a tradeable session (not Asian)."""
    return get_session() != "Asian"


# ═══════════════════════════════════════════
# MT5 CONNECTION
# ═══════════════════════════════════════════
def connect_mt5() -> bool:
    """
    Initialise connection to MetaTrader 5 using credentials from .env file.

    Returns:
        True if connection succeeded, False otherwise.
    """
    if not mt5.initialize(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
        print("MT5 connection failed:", mt5.last_error())
        return False
    return True


# ═══════════════════════════════════════════
# DYNAMIC FILLING MODE
# ═══════════════════════════════════════════
def get_filling_mode(symbol: str) -> int:
    """
    Detect the correct order filling mode for a symbol.
    Brokers differ — ICMarkets uses IOC, others may use FOK or RETURN.

    Args:
        symbol: Trading symbol string

    Returns:
        MT5 order filling mode constant
    """
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_IOC   # default to IOC if symbol info unavailable
    filling = info.filling_mode
    if filling & 2:
        return mt5.ORDER_FILLING_IOC   # Immediate Or Cancel
    elif filling & 1:
        return mt5.ORDER_FILLING_FOK   # Fill Or Kill
    else:
        return mt5.ORDER_FILLING_RETURN


# ═══════════════════════════════════════════
# SMART LOT SIZING
# ═══════════════════════════════════════════
def calc_lot_size(symbol: str, risk_amount_account: float, sl_distance_price: float) -> float:
    """
    Calculate lot size so that the SL distance equals exactly the risk amount.
    Uses tick size and tick value from MT5 symbol info for accuracy.

    Args:
        symbol:               Trading symbol
        risk_amount_account:  Dollar amount to risk (e.g. balance * 0.01)
        sl_distance_price:    Distance in price between entry and SL

    Returns:
        Lot size rounded to broker's volume step, clamped to min/max.
    """
    info = mt5.symbol_info(symbol)
    if info is None or sl_distance_price <= 0:
        return 0.01   # fallback to minimum lot if data unavailable

    tick_size  = info.trade_tick_size   # smallest price movement
    tick_value = info.trade_tick_value  # dollar value of one tick per lot

    if tick_size <= 0 or tick_value <= 0:
        return 0.01

    ticks_in_sl   = sl_distance_price / tick_size     # how many ticks is our SL
    value_per_lot = ticks_in_sl * tick_value           # dollar risk for 1 lot

    if value_per_lot <= 0:
        return 0.01

    lot = risk_amount_account / value_per_lot          # lots needed to risk exactly our amount

    # clamp to broker's allowed lot range and round to volume step
    lot = max(info.volume_min,
              min(info.volume_max,
                  round(lot / info.volume_step) * info.volume_step))
    return round(lot, 2)


# ═══════════════════════════════════════════
# GET CANDLES FROM MT5
# ═══════════════════════════════════════════
def get_candles(timeframe: int, count: int = 500, symbol: str = None) -> pd.DataFrame:
    """
    Fetch OHLCV candle data from MT5 and return as a pandas DataFrame.

    Args:
        timeframe: MT5 timeframe constant e.g. mt5.TIMEFRAME_M30
        count:     Number of candles to fetch (default 500)
        symbol:    Symbol to fetch (defaults to global SYMBOL)

    Returns:
        DataFrame with columns: time, open, high, low, close, tick_volume, spread, real_volume
        Returns None if data unavailable.
    """
    sym   = symbol or SYMBOL
    rates = mt5.copy_rates_from_pos(sym, timeframe, 0, count)  # fetch from most recent bar backwards
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)  # convert Unix timestamp to UTC datetime
    return df


# ═══════════════════════════════════════════
# INDICATOR CALCULATIONS
# ═══════════════════════════════════════════
def calc_ema(series: pd.Series, period: int) -> pd.Series:
    """
    Calculate Exponential Moving Average using pandas ewm.
    adjust=False matches TradingView/Pine Script EMA calculation exactly.
    """
    return series.ewm(span=period, adjust=False).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate RSI (Relative Strength Index) using Wilder's smoothing method.
    ewm(span=period) approximates Wilder's smoothing.
    """
    delta    = series.diff()
    gain     = delta.where(delta > 0, 0)   # keep positive moves, zero out negatives
    loss     = -delta.where(delta < 0, 0)  # keep negative moves (as positive), zero out positives
    avg_gain = gain.ewm(span=period).mean()
    avg_loss = loss.ewm(span=period).mean()
    rs       = avg_gain / avg_loss         # relative strength ratio
    return 100 - (100 / (1 + rs))          # convert to 0-100 oscillator


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate Average True Range (ATR) — measures volatility.
    True Range = max of: (high-low), abs(high-prev_close), abs(low-prev_close)
    """
    hl = df['high'] - df['low']                    # candle range
    hc = abs(df['high'] - df['close'].shift())     # gap up scenario
    lc = abs(df['low']  - df['close'].shift())     # gap down scenario
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)  # true range is the largest of the three
    return tr.ewm(span=period).mean()              # smooth with EMA


def calc_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate ADX (Average Directional Index) — measures trend strength.
    ADX > 25 = trending market. ADX < 25 = ranging/choppy market.
    """
    plus_dm  = df['high'].diff()      # upward price movement
    minus_dm = df['low'].diff().abs() # downward price movement
    plus_dm[plus_dm   < 0] = 0        # only keep positive upward moves
    minus_dm[minus_dm < 0] = 0        # only keep positive downward moves
    atr_val  = calc_atr(df, period)
    plus_di  = 100 * (plus_dm.ewm(span=period).mean()  / atr_val)   # directional indicator +
    minus_di = 100 * (minus_dm.ewm(span=period).mean() / atr_val)   # directional indicator -
    dx       = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100  # directional index
    return dx.ewm(span=period).mean()  # ADX = smoothed DX


def calc_macd(series: pd.Series) -> tuple:
    """
    Calculate MACD line and signal line.
    MACD = 12-period EMA minus 26-period EMA.
    Signal = 9-period EMA of MACD.

    Returns:
        Tuple of (macd_line, signal_line) as pandas Series.
    """
    macd   = series.ewm(span=12).mean() - series.ewm(span=26).mean()
    signal = macd.ewm(span=9).mean()
    return macd, signal


# ═══════════════════════════════════════════
# STRONG BODY CHECK
# ─────────────────────────────────────────────
# Matches Pine Script c4H_strongBody:
# A valid bias confirmation candle must have body >= 60% of its total range.
# This filters out wick spikes that close outside the bias range but with no conviction.
# ═══════════════════════════════════════════
def is_strong_body(
    candle_open: float,
    candle_close: float,
    candle_high: float,
    candle_low: float,
    threshold: float = 0.60,
) -> tuple:
    """
    Check whether a candle has a strong body (>=60% of total range).

    Args:
        candle_open:  Open price
        candle_close: Close price
        candle_high:  High price
        candle_low:   Low price
        threshold:    Minimum body/range ratio (default 0.60 = 60%)

    Returns:
        Tuple of (is_strong, is_bull_strong, is_bear_strong)
    """
    c_range = candle_high - candle_low
    if c_range <= 0:
        return False, False, False

    c_body      = abs(candle_close - candle_open)
    strong      = (c_body / c_range) >= threshold   # body covers at least 60% of range
    bull_strong = strong and candle_close > candle_open   # bullish AND strong
    bear_strong = strong and candle_close < candle_open   # bearish AND strong
    return strong, bull_strong, bear_strong


# ═══════════════════════════════════════════
# WEEKLY BIAS — PURE 4H CANDLE DETECTION
# ─────────────────────────────────────────────
# Candle 1: 4H bar opening at 00:00 UTC Monday (all pairs, all seasons)
# Candle 2: 4H bar opening at 04:00 UTC Monday (all pairs, all seasons)
# Range: wb_high = MAX(c1_high, c2_high) | wb_low = MIN(c1_low, c2_low)
# Confirmation: first 4H close AFTER candle 2 with ≥60% strong body break
# Auto-resets each new ISO week
# ═══════════════════════════════════════════
def get_weekly_bias(df_4h: pd.DataFrame, symbol: str = "XAUUSD") -> tuple:
    """
    Calculate weekly bias range and confirmation from 4H candles.

    Args:
        df_4h:  4H OHLCV DataFrame
        symbol: Symbol name (used for logging only)

    Returns:
        Tuple: (wb_high, wb_low, wk_bull, wk_bear, price_in_range, wk_partial, wk_full)
    """
    df = df_4h.copy()
    df['dow']  = df['time'].dt.dayofweek   # Monday = 0 in Python
    df['hour'] = df['time'].dt.hour

    if len(df) == 0:
        return None, None, False, False, False, False, False

    # Filter to current ISO week only
    latest_week = df['time'].dt.isocalendar().week.iloc[-1]
    latest_year = df['time'].dt.isocalendar().year.iloc[-1]
    this_week   = df[
        (df['time'].dt.isocalendar().week == latest_week) &
        (df['time'].dt.isocalendar().year == latest_year)
    ]

    if len(this_week) == 0:
        return None, None, False, False, False, False, False

    # Candle 1: Monday 00:00 UTC (BIAS_CANDLE1_HOUR = 0)
    c1_rows = this_week[(this_week['dow'] == 0) & (this_week['hour'] == BIAS_CANDLE1_HOUR)]
    # Candle 2: Monday 04:00 UTC (BIAS_CANDLE2_HOUR = 4)
    c2_rows = this_week[(this_week['dow'] == 0) & (this_week['hour'] == BIAS_CANDLE2_HOUR)]

    if len(c1_rows) == 0:
        return None, None, False, False, False, False, False

    c1 = c1_rows.iloc[-1]  # most recent matching candle

    wb_high    = None
    wb_low     = None
    wk_partial = False
    wk_full    = False

    if len(c2_rows) == 0:
        # Only candle 1 has formed — partial range (before 04:00 UTC Monday)
        wb_high    = c1['high']
        wb_low     = c1['low']
        wk_partial = True
        wk_full    = False
    else:
        # Both candles formed — use MAX high and MIN low across both
        c2         = c2_rows.iloc[-1]
        wb_high    = max(c1['high'], c2['high'])
        wb_low     = min(c1['low'],  c2['low'])
        wk_full    = True
        wk_partial = False

    current_close  = df['close'].iloc[-1]
    price_in_range = wb_low < current_close < wb_high  # True if price inside bias range

    # Check for strong-body confirmation candles AFTER the bias candles
    wk_bull = False
    wk_bear = False

    if wk_full and len(c2_rows) > 0:
        c2_time         = c2_rows.iloc[-1]['time']
        confirm_candles = this_week[this_week['time'] > c2_time]  # only look at candles after bias forms

        for _, row in confirm_candles.iterrows():
            _, bull_strong, bear_strong = is_strong_body(
                row['open'], row['close'], row['high'], row['low']
            )
            # Bull confirmation: close above wb_high with strong bullish body
            if not wk_bull and row['close'] > wb_high and bull_strong:
                wk_bull = True
            # Bear confirmation: close below wb_low with strong bearish body
            if not wk_bear and row['close'] < wb_low and bear_strong:
                wk_bear = True
            if wk_bull or wk_bear:
                break   # first confirmation wins — stop checking

    return wb_high, wb_low, wk_bull, wk_bear, price_in_range, wk_partial, wk_full


# ═══════════════════════════════════════════
# PRIORITY 2 — DAILY BIAS (FALLBACK REMOVED)
# ─────────────────────────────────────────────
# CHANGE: Removed fallback that grabbed first 2 candles when midnight candle
# range was < 0.03% of price. Pine Script has NO such fallback.
# That fallback caused Python and Pine Script to disagree on daily bias
# during quiet Asian sessions — breaking the entire verification system.
#
# FIX: Always use the candle at 00:00 UTC, no exceptions.
# Small range is market information, not a bug.
# ═══════════════════════════════════════════
def get_daily_bias(df_4h: pd.DataFrame, symbol: str = "XAUUSD") -> tuple:
    """
    Calculate daily bias from the 4H candle that opens at 00:00 UTC each day.
    Always uses exactly the midnight UTC candle — no fallbacks.
    Matches Pine Script get_daily_bias logic exactly.

    Args:
        df_4h:  4H OHLCV DataFrame
        symbol: Symbol name (used for logging only)

    Returns:
        Tuple: (db_high, db_low, day_bull, day_bear)
    """
    df = df_4h.copy()
    df['date'] = df['time'].dt.date   # extract date part for grouping
    df['hour'] = df['time'].dt.hour   # extract hour for filtering

    today         = df['date'].iloc[-1]      # most recent date in data
    today_candles = df[df['date'] == today]  # filter to today's candles only

    # Always use the candle at 00:00 UTC — no fallback logic
    # NOTE: Small range is market information, not a bug. Do not substitute.
    c1_rows = today_candles[today_candles['hour'] == BIAS_CANDLE1_HOUR]  # hour == 0

    if len(c1_rows) == 0:
        return None, None, False, False   # 00:00 candle hasn't formed yet

    c1      = c1_rows.iloc[-1]   # use the midnight candle
    db_high = c1['high']         # daily bias high = candle high
    db_low  = c1['low']          # daily bias low  = candle low

    # REMOVED: fallback logic that grabbed first 2 candles when range was small
    # Reason: Pine Script does not have this fallback. Including it causes
    # Python and Pine Script to disagree on daily bias during quiet Asian sessions.

    # Now look for confirmation candles after 00:00 UTC today
    confirm_candles = today_candles[today_candles['time'] > c1['time']]

    day_bull = False
    day_bear = False

    for _, row in confirm_candles.iterrows():
        _, bull_strong, bear_strong = is_strong_body(
            row['open'], row['close'], row['high'], row['low']
        )
        # Bull confirmation: strong bullish close above daily high
        if not day_bull and row['close'] > db_high and bull_strong:
            day_bull = True
        # Bear confirmation: strong bearish close below daily low
        if not day_bear and row['close'] < db_low and bear_strong:
            day_bear = True
        if day_bull or day_bear:
            break   # first confirmation wins

    return db_high, db_low, day_bull, day_bear


# ═══════════════════════════════════════════
# HTF EMA CONFIRMATION
# ═══════════════════════════════════════════
def get_htf_ema(df_1h: pd.DataFrame, df_4h: pd.DataFrame) -> tuple:
    """
    Check whether 1H and 4H closes are above/below their EMA200.
    Returns bullish/bearish confirmation for higher timeframe trend.

    Returns:
        Tuple: (htf_bull, htf_bear)
    """
    ema200_1h = calc_ema(df_1h['close'], 200).iloc[-1]  # 1H EMA200 current value
    ema200_4h = calc_ema(df_4h['close'], 200).iloc[-1]  # 4H EMA200 current value
    c1h = df_1h['close'].iloc[-1]   # most recent closed 1H bar
    c4h = df_4h['close'].iloc[-1]   # most recent closed 4H bar
    htf_bull = (c1h > ema200_1h) or (c4h > ema200_4h)   # either TF bullish = bullish
    htf_bear = (c1h < ema200_1h) or (c4h < ema200_4h)   # either TF bearish = bearish
    return htf_bull, htf_bear


# ═══════════════════════════════════════════
# PREVIOUS DAY/WEEK/MONTH HIGH AND LOW
# ═══════════════════════════════════════════
def get_prev_day_hl(df: pd.DataFrame) -> tuple:
    """Get previous day's high and low for sweep/PDH/PDL detection."""
    df = df.copy()
    df['date'] = df['time'].dt.date
    dates = sorted(df['date'].unique())
    if len(dates) < 2:
        return None, None
    prev = df[df['date'] == dates[-2]]   # second to last date = previous day
    return prev['high'].max(), prev['low'].min()


def get_prev_week_hl(df: pd.DataFrame) -> tuple:
    """Get previous week's high and low for premium/discount zone calculation."""
    df = df.copy()
    df['week'] = df['time'].dt.isocalendar().week
    df['year'] = df['time'].dt.isocalendar().year
    weeks = df[['year','week']].drop_duplicates().values.tolist()
    if len(weeks) < 2:
        return None, None
    py, pw = weeks[-2]   # second to last week = previous week
    prev = df[(df['year'] == py) & (df['week'] == pw)]
    return prev['high'].max(), prev['low'].min()


def get_prev_month_hl(df: pd.DataFrame) -> tuple:
    """Get previous month's high and low for monthly alignment check."""
    df = df.copy()
    df['month'] = df['time'].dt.month
    df['year']  = df['time'].dt.year
    months = df[['year','month']].drop_duplicates().values.tolist()
    if len(months) < 2:
        return None, None
    py, pm = months[-2]   # second to last month = previous month
    prev = df[(df['year'] == py) & (df['month'] == pm)]
    return prev['high'].max(), prev['low'].min()


# ═══════════════════════════════════════════
# PRIORITY 3 — BOS DETECTION (TIMEFRAME-AWARE RESET)
# ─────────────────────────────────────────────
# CHANGE: BOS lookback window is now timeframe-dependent.
# Previously: always 30 bars regardless of timeframe.
# Problem: 30 bars on 5M = only 2.5 hours — BOS resets too fast, catches fakes.
#          30 bars on 4H = 5 days — too long, BOS stays valid too long.
#
# FIX: Match Pine Script logic using MT5 integer constants (not strings).
# Using MT5 constants avoids any case sensitivity issues with string comparison.
# MT5 constants are just integers — no upper/lower case ambiguity possible.
#
#   mt5.TIMEFRAME_M5  → bosRst = 50 (isScalpTF — small TFs need more bars for context)
#   mt5.TIMEFRAME_M15 → bosRst = 30 (isDayTF — standard lookback)
#   mt5.TIMEFRAME_M30 → bosRst = 30 (isDayTF — standard lookback)
#   mt5.TIMEFRAME_H1  → bosRst = 20 (isSwingTF — larger TFs reset faster)
#   mt5.TIMEFRAME_H4  → bosRst = 20 (isSwingTF — larger TFs reset faster)
# ═══════════════════════════════════════════
def get_bos_reset(timeframe: int) -> int:
    """
    Return the BOS reset lookback appropriate for the given timeframe.
    Matches Pine Script bosRst logic exactly.

    Uses MT5 integer constants directly — NOT string comparisons — so there
    is zero risk of case sensitivity bugs (e.g. '5m' vs '5M' failing to match).
    MT5 constants like mt5.TIMEFRAME_M5 are just integers under the hood.

    Args:
        timeframe: MT5 timeframe integer constant (e.g. mt5.TIMEFRAME_M5)

    Returns:
        Integer number of bars for BOS reset window.
    """
    if timeframe == mt5.TIMEFRAME_M5:
        # Scalp timeframe: 50 bars = ~4 hours on 5M
        # Need MORE bars because 5M candles are tiny — 30 bars only covers 2.5h
        return 50

    elif timeframe in (mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M30):
        # Day trade timeframes: standard 30-bar reset
        # 30 bars = 7.5h on 15M | 15h on 30M — solid context window
        return 30

    else:
        # Swing timeframes (1H, 4H, Daily): tighter 20-bar reset
        # 20 bars = 20h on 1H | 80h on 4H — don't need as many bars at larger TFs
        return 20


def check_bos(df: pd.DataFrame, atr_series: pd.Series, timeframe: int) -> tuple:
    """
    Detect recent Break of Structure (BOS) in either direction.
    Uses timeframe-appropriate lookback window to prevent phantom signals.

    Args:
        df:         OHLCV DataFrame for the signal timeframe
        atr_series: ATR Series for the same timeframe
        timeframe:  MT5 timeframe constant — determines lookback window

    Returns:
        Tuple: (rec_bull_bos, rec_bear_bos) — both booleans
    """
    # Get the correct lookback for this timeframe (PRIORITY 3 fix)
    bos_reset = get_bos_reset(timeframe)

    close  = df['close']
    open_  = df['open']
    ema200 = calc_ema(close, 200)   # EMA200 for trend direction filter

    # Swing high/low using 11-bar rolling window (5 bars each side + centre)
    sw_high     = df['high'].rolling(11, center=True).max()
    sw_low      = df['low'].rolling(11,  center=True).min()

    strong_body = abs(close - open_) > atr_series * 0.5   # candle must close strongly
    above_e200  = close > ema200
    below_e200  = close < ema200

    # Bull BOS: close above swing high with strong body, while price is above EMA200
    bull_bos = (close > sw_high.shift(1)) & strong_body & above_e200 & (close.shift(1) <= sw_high.shift(1))
    # Bear BOS: close below swing low with strong body, while price is below EMA200
    bear_bos = (close < sw_low.shift(1))  & strong_body & below_e200 & (close.shift(1) >= sw_low.shift(1))

    # Check only within the timeframe-appropriate lookback window
    return bull_bos.iloc[-bos_reset:].any(), bear_bos.iloc[-bos_reset:].any()


# ═══════════════════════════════════════════
# SWEEP DETECTION
# ═══════════════════════════════════════════
def check_sweeps(df: pd.DataFrame, atr_series: pd.Series, d1h: float, d1l: float) -> tuple:
    """
    Detect liquidity sweeps above previous day high or below previous day low.
    A sweep is when price pierces a level but then closes back inside it (rejection).

    Returns:
        Tuple: (bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej, sw_low, sw_high)
    """
    if d1h is None or d1l is None:
        return False, False, False, False, None, None

    last = df.iloc[-2]    # use second to last bar (fully closed candle)
    atr  = atr_series.iloc[-2]

    # Sweep below previous day low — could be a bull trap before reversal
    bull_sweep  = (last['low'] < d1l)  and (last['close'] > d1l)  and ((d1l - last['low'])   >= atr * 0.3)
    # Sweep above previous day high — could be a bear trap before reversal
    bear_sweep  = (last['high'] > d1h) and (last['close'] < d1h)  and ((last['high'] - d1h)  >= atr * 0.3)

    # Strong rejection = close back well inside the range (> 0.7 ATR recovery)
    bull_sw_rej = bull_sweep and ((last['close'] - last['low'])   > atr * 0.7)
    bear_sw_rej = bear_sweep and ((last['high']  - last['close']) > atr * 0.7)

    return (bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej,
            last['low']  if bull_sweep else None,
            last['high'] if bear_sweep else None)


# ═══════════════════════════════════════════
# INTERNAL SWEEP DETECTION
# ═══════════════════════════════════════════
def check_internal_sweep(df: pd.DataFrame, above_e200: bool, below_e200: bool) -> tuple:
    """
    Detect internal structure sweeps (short-term liquidity grabs).
    Looks at last 3 candles for a sweep of recent highs/lows.

    Returns:
        Tuple: (int_bull, int_bear)
    """
    low3  = df['low'].iloc[-4:-1].min()    # lowest low of last 3 closed candles
    high3 = df['high'].iloc[-4:-1].max()   # highest high of last 3 closed candles
    last  = df.iloc[-2]

    int_bull = (last['low'] < low3)   and (last['close'] > low3)  and above_e200
    int_bear = (last['high'] > high3) and (last['close'] < high3) and below_e200
    return int_bull, int_bear


# ═══════════════════════════════════════════
# EQUAL HIGHS / LOWS SWEPT
# ═══════════════════════════════════════════
def check_eql_swept(df: pd.DataFrame, atr: float) -> tuple:
    """
    Detect when equal highs (EQH) or equal lows (EQL) have been swept.
    Equal highs/lows are levels where price double-topped or double-bottomed.
    When swept, it signals liquidity was grabbed — potential reversal zone.

    Returns:
        Tuple: (eqh_swept, eql_swept)
    """
    ph  = df['high'].iloc[-12]   # pivot high approximately 12 bars ago
    pph = df['high'].iloc[-23]   # previous pivot high approximately 23 bars ago
    pl  = df['low'].iloc[-12]    # pivot low approximately 12 bars ago
    ppl = df['low'].iloc[-23]    # previous pivot low approximately 23 bars ago
    last    = df.iloc[-2]

    # Equal highs: two pivot highs within 10% of ATR of each other
    is_eqh  = (abs(ph - pph) <= atr * 0.1) if not (pd.isna(ph) or pd.isna(pph)) else False
    # Equal lows: two pivot lows within 10% of ATR of each other
    is_eql  = (abs(pl - ppl) <= atr * 0.1) if not (pd.isna(pl) or pd.isna(ppl)) else False

    # Swept = price went through the level but closed back inside (wick above/below, close inside)
    eqh_swept = is_eqh and (last['high'] > ph) and (last['close'] < ph)
    eql_swept = is_eql and (last['low']  < pl) and (last['close'] > pl)
    return eqh_swept, eql_swept


# ═══════════════════════════════════════════
# FAIR VALUE GAP (FVG) DETECTION
# ═══════════════════════════════════════════
def check_fvg(
    df: pd.DataFrame,
    atr_series: pd.Series,
    above_e200: bool,
    below_e200: bool,
    wk_bull: bool,
    wk_bear: bool,
    day_bull: bool,
    day_bear: bool,
    bias_med_bull: bool,
    bias_med_bear: bool,
    bias_sca_bull: bool,
    bias_sca_bear: bool,
) -> tuple:
    """
    Detect Fair Value Gap (FVG) — a three-candle imbalance pattern.
    A bull FVG is a gap between candle[-3] high and candle[-1] low (price skipped over).
    A bear FVG is a gap between candle[-3] low and candle[-1] high.

    Returns:
        Tuple: (bull_fvg_ez, bear_fvg_ez, bull_fvg_bot, bear_fvg_top)
    """
    if len(df) < 6:
        return False, False, None, None

    c0  = df.iloc[-1]   # current (forming) candle — for entry zone check
    c1  = df.iloc[-2]   # last closed candle
    c2  = df.iloc[-3]   # impulse candle
    c3  = df.iloc[-4]   # FVG reference candle

    atr   = atr_series.iloc[-3]
    body  = abs(c2['close'] - c2['open'])
    str_cdl = body > atr * 1.0   # impulse candle must have significant body

    b_fvg_sz  = c1['low']  - c3['high']   # bull FVG size = gap between c1 low and c3 high
    br_fvg_sz = c3['low']  - c1['high']   # bear FVG size = gap between c3 low and c1 high

    fvg_min = 0.5   # FVG must be at least 0.5x ATR in size to count
    b_fvg   = (b_fvg_sz  > 0) and str_cdl and (c2['close'] > c2['open']) and above_e200 and (b_fvg_sz  >= atr * fvg_min)
    br_fvg  = (br_fvg_sz > 0) and str_cdl and (c2['close'] < c2['open']) and below_e200 and (br_fvg_sz >= atr * fvg_min)

    disp_cdl = abs(c0['close'] - c0['open']) > atr_series.iloc[-1] * 1.5   # current candle is displacement

    # Entry zone: price has entered the FVG
    bull_in_fvg = b_fvg  and (c0['low'] <= c1['low'])   and (c0['close'] >= c3['high'])
    bear_in_fvg = br_fvg and (c0['high'] >= c1['high'])  and (c0['close'] <= c3['low'])

    # Full entry zone signal: in FVG, displacement candle, correct bias
    bull_fvg_ez = bull_in_fvg and disp_cdl and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull)
    bear_fvg_ez = bear_in_fvg and disp_cdl and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear)

    bull_fvg_bot = c3['high'] if b_fvg else None   # bottom of bull FVG (SL reference)
    bear_fvg_top = c3['low']  if br_fvg else None  # top of bear FVG (SL reference)

    return bull_fvg_ez, bear_fvg_ez, bull_fvg_bot, bear_fvg_top


# ═══════════════════════════════════════════
# PRIORITY 4 — STATEFUL ORDER BLOCK DETECTION (INDEX CORRECTED)
# ─────────────────────────────────────────────
# CHANGE: OB candle index corrected from iloc[-6] to iloc[-5]
#
# REASON: Impulse is detected on iloc[-2] through iloc[-5] (3 consecutive candles).
# The OB candle is the one immediately BEFORE the impulse — which is iloc[-5],
# not iloc[-6]. Using iloc[-6] was tagging the wrong candle as the Order Block,
# causing entry/SL zones to be off by one bar.
#
# Pine Script uses: close[imp3] where imp3=3, relative to current bar.
# That maps to iloc[-4] for impulse start, meaning OB is iloc[-5].
# ═══════════════════════════════════════════
def check_ob_stateful(
    df: pd.DataFrame,
    atr_series: pd.Series,
    above_e200: bool,
    below_e200: bool,
    ob_state: dict,
    key: str,
) -> tuple:
    """
    Detect and track Order Blocks with persistent state between scans.
    An OB is the last opposing candle before a strong impulse move.
    State is maintained so we know if an OB has been touched or invalidated.

    Args:
        df:         OHLCV DataFrame for signal timeframe
        atr_series: ATR Series for the same timeframe
        above_e200: Whether close is above EMA200
        below_e200: Whether close is below EMA200
        ob_state:   Persistent dictionary tracking active OBs across scans
        key:        Unique key for this symbol+timeframe combination

    Returns:
        Tuple: (bull_ob_rsp, bear_ob_rsp, bull_ob_bot, bear_ob_top)
    """
    if len(df) < 8:
        return False, False, None, None

    close  = df['close']
    open_  = df['open']
    atr    = atr_series.iloc[-2]
    n      = len(df)   # total bar count (used to calculate OB age)

    # Detect bullish impulse: 3 consecutive higher closes (bars -2, -3, -4 relative to current)
    bull_imp = (close.iloc[-2] > close.iloc[-3] and
                close.iloc[-3] > close.iloc[-4] and
                close.iloc[-4] > close.iloc[-5])

    # Detect bearish impulse: 3 consecutive lower closes
    bear_imp = (close.iloc[-2] < close.iloc[-3] and
                close.iloc[-3] < close.iloc[-4] and
                close.iloc[-4] < close.iloc[-5])

    # PRIORITY 4 FIX: OB candle is iloc[-5], NOT iloc[-6]
    # Impulse starts at iloc[-5], so the OB is the candle just before impulse = iloc[-5]
    # Previously was iloc[-6] which was one bar too far back — wrong candle tagged
    if bull_imp and above_e200 and len(df) >= 6:
        ob_candle = df.iloc[-5]   # ✅ FIXED: was iloc[-6], now iloc[-5]
        # OB candle must be bearish (opposite to impulse direction) with significant body
        if (ob_candle['close'] < ob_candle['open'] and
                abs(ob_candle['close'] - ob_candle['open']) > atr * 0.5):
            existing = ob_state[key].get('bull_ob')
            # Only register new OB if no active one exists
            if not existing or not existing.get('active', False):
                ob_state[key]['bull_ob'] = {
                    'top': ob_candle['high'], 'bot': ob_candle['low'],
                    'active': True, 'touched': False,
                    'bar_formed': n, 'formed_at': str(ob_candle['time'])
                }

    if bear_imp and below_e200 and len(df) >= 6:
        ob_candle = df.iloc[-5]   # ✅ FIXED: was iloc[-6], now iloc[-5]
        # OB candle must be bullish (opposite to impulse direction) with significant body
        if (ob_candle['close'] > ob_candle['open'] and
                abs(ob_candle['close'] - ob_candle['open']) > atr * 0.5):
            existing = ob_state[key].get('bear_ob')
            if not existing or not existing.get('active', False):
                ob_state[key]['bear_ob'] = {
                    'top': ob_candle['high'], 'bot': ob_candle['low'],
                    'active': True, 'touched': False,
                    'bar_formed': n, 'formed_at': str(ob_candle['time'])
                }

    bull_ob_rsp = False
    bear_ob_rsp = False
    bull_ob_bot = None
    bear_ob_top = None
    current = df.iloc[-2]   # last fully closed candle

    # Check if price has entered the bull OB zone (response / retest)
    bob = ob_state[key].get('bull_ob')
    if bob and bob.get('active') and not bob.get('touched'):
        if current['low'] <= bob['top'] and current['close'] >= bob['bot']:
            bull_ob_rsp = True
            bull_ob_bot = bob['bot']
            ob_state[key]['bull_ob']['touched'] = True   # mark as touched (one-touch rule)
        if current['close'] < bob['bot']:
            ob_state[key]['bull_ob']['active'] = False   # price closed below OB — invalidated

    # Check if price has entered the bear OB zone
    bearob = ob_state[key].get('bear_ob')
    if bearob and bearob.get('active') and not bearob.get('touched'):
        if current['high'] >= bearob['bot'] and current['close'] <= bearob['top']:
            bear_ob_rsp = True
            bear_ob_top = bearob['top']
            ob_state[key]['bear_ob']['touched'] = True
        if current['close'] > bearob['top']:
            ob_state[key]['bear_ob']['active'] = False   # price closed above OB — invalidated

    # Expire OBs that are more than 50 bars old — they lose significance over time
    if bob and (n - bob.get('bar_formed', 0)) > 50:
        ob_state[key]['bull_ob']['active'] = False
    if bearob and (n - bearob.get('bar_formed', 0)) > 50:
        ob_state[key]['bear_ob']['active'] = False

    return bull_ob_rsp, bear_ob_rsp, bull_ob_bot, bear_ob_top


# ═══════════════════════════════════════════
# RSI DIVERGENCE DETECTION
# ═══════════════════════════════════════════
def check_rsi_div(df: pd.DataFrame, rsi_series: pd.Series) -> tuple:
    """
    Detect RSI divergence — price and RSI moving in opposite directions.
    Strong divergence: price makes lower low but RSI makes higher low (bullish).
    Hidden divergence: price makes higher low but RSI makes lower low (bullish continuation).

    Returns:
        Tuple: (s_bull_div, h_bull_div, s_bear_div, h_bear_div)
    """
    lkb  = 7   # lookback window (7 bars)
    low  = df['low']
    high = df['high']

    # Price structure comparisons across two 7-bar windows
    p_ll = low.iloc[-lkb:].min()   < low.iloc[-2*lkb:-lkb].min()    # price lower low
    p_hl = low.iloc[-lkb:].min()   > low.iloc[-2*lkb:-lkb].min()    # price higher low
    p_hh = high.iloc[-lkb:].max()  > high.iloc[-2*lkb:-lkb].max()   # price higher high
    p_lh = high.iloc[-lkb:].max()  < high.iloc[-2*lkb:-lkb].max()   # price lower high

    # RSI structure comparisons across same two windows
    r_hl = rsi_series.iloc[-lkb:].min() > rsi_series.iloc[-2*lkb:-lkb].min()   # RSI higher low
    r_ll = rsi_series.iloc[-lkb:].min() < rsi_series.iloc[-2*lkb:-lkb].min()   # RSI lower low
    r_lh = rsi_series.iloc[-lkb:].max() < rsi_series.iloc[-2*lkb:-lkb].max()   # RSI lower high
    r_hh = rsi_series.iloc[-lkb:].max() > rsi_series.iloc[-2*lkb:-lkb].max()   # RSI higher high

    # Strong bull div: price lower low + RSI higher low (momentum diverging bullishly)
    # Hidden bull div: price higher low + RSI lower low (trend continuation signal)
    return (p_ll and r_hl), (p_hl and r_ll), (p_hh and r_lh), (p_lh and r_hh)


# ═══════════════════════════════════════════
# STRUCTURE-BASED SL / TP
# ─────────────────────────────────────────────
# Priority order for SL: OB → FVG → 15-bar swing → ATR fallback
# Session buffer added to SL to avoid being stopped by spread/noise
# TP varies by tier: STRONG=1:3, MEDIUM=towards weekly level, SCALP=1.5R minimum
# ═══════════════════════════════════════════
def calc_structure_sl_tp(
    close: float,
    atr: float,
    t1_bull: bool,
    t2_bull: bool,
    t1_bear: bool,
    t2_bear: bool,
    bull_ob_bot: float,
    bear_ob_top: float,
    bull_fvg_bot: float,
    bear_fvg_top: float,
    df: pd.DataFrame,
    wb_high: float,
    wb_low: float,
    bull_tier: str,
    bear_tier: str,
    rr: float = 3.0,
) -> tuple:
    """
    Calculate stop loss and take profit prices based on market structure.
    Matches Pine Script SL/TP logic exactly including session buffers.

    Returns:
        Tuple: (long_sl, long_risk, long_tp, sl_src_bull,
                short_sl, short_risk, short_tp, sl_src_bear)
    """
    session = get_session()

    # Session buffer: tighter during prime sessions, wider during quiet times
    if session in ("London", "New York", "Overlap"):
        sess_buf = atr * 0.2   # tight buffer — precise entries during high volume
    elif session == "Post-NY":
        sess_buf = atr * 0.3   # slightly wider — a bit more noise in quiet hours
    else:
        sess_buf = atr * 0.5   # widest buffer — Asian session is unpredictable

    # ── LONG SL (priority: OB → FVG → swing → ATR) ──
    local_swing_low = df['low'].iloc[-17:-2].min()   # lowest low of last 15 closed bars
    long_sl_atr     = close - atr * 1.5              # ATR fallback SL

    if t2_bull and bull_ob_bot is not None:
        long_sl     = bull_ob_bot - sess_buf    # SL just below the Order Block bottom
        sl_src_bull = "OB"
    elif t1_bull and bull_fvg_bot is not None:
        long_sl     = bull_fvg_bot - sess_buf   # SL just below the FVG bottom
        sl_src_bull = "FVG"
    elif (local_swing_low - sess_buf) > (close - atr * 3):
        long_sl     = local_swing_low - sess_buf    # SL below recent swing low
        sl_src_bull = "Swing"
    else:
        long_sl     = long_sl_atr    # ATR-based SL (no structural reference available)
        sl_src_bull = "ATR"

    long_risk = max(close - long_sl, 0.0001)   # distance from entry to SL

    # ── SHORT SL (priority: OB → FVG → swing → ATR) ──
    local_swing_high = df['high'].iloc[-17:-2].max()  # highest high of last 15 closed bars
    short_sl_atr     = close + atr * 1.5

    if t2_bear and bear_ob_top is not None:
        short_sl     = bear_ob_top + sess_buf   # SL just above the Order Block top
        sl_src_bear  = "OB"
    elif t1_bear and bear_fvg_top is not None:
        short_sl     = bear_fvg_top + sess_buf  # SL just above the FVG top
        sl_src_bear  = "FVG"
    elif (local_swing_high + sess_buf) < (close + atr * 3):
        short_sl     = local_swing_high + sess_buf   # SL above recent swing high
        sl_src_bear  = "Swing"
    else:
        short_sl     = short_sl_atr
        sl_src_bear  = "ATR"

    short_risk = max(short_sl - close, 0.0001)

    # ── TP CALCULATION (varies by signal tier) ──
    long_tp_base  = close + long_risk  * rr   # base TP = 1:3 from SL
    short_tp_base = close - short_risk * rr

    # STRONG: pure 1:3 from structure SL
    if bull_tier == "STRONG":
        long_tp = long_tp_base
    # MEDIUM: target weekly bias level if it's beyond 1:3
    elif bull_tier == "MEDIUM":
        long_tp = wb_high if (wb_high is not None and wb_high >= long_tp_base) else long_tp_base
    # SCALP: target weekly level only if RR is at least 1.5
    elif bull_tier == "SCALP":
        long_tp_sca = wb_high if (wb_high is not None and wb_high >= long_tp_base) else long_tp_base
        long_rr_sca = (long_tp_sca - close) / long_risk if long_risk > 0 else 0
        long_tp     = long_tp_sca if long_rr_sca >= SCALP_MIN_RR else long_tp_base
    else:
        long_tp = long_tp_base

    if bear_tier == "STRONG":
        short_tp = short_tp_base
    elif bear_tier == "MEDIUM":
        short_tp = wb_low if (wb_low is not None and wb_low <= short_tp_base) else short_tp_base
    elif bear_tier == "SCALP":
        short_tp_sca = wb_low if (wb_low is not None and wb_low <= short_tp_base) else short_tp_base
        short_rr_sca = (close - short_tp_sca) / short_risk if short_risk > 0 else 0
        short_tp     = short_tp_sca if short_rr_sca >= SCALP_MIN_RR else short_tp_base
    else:
        short_tp = short_tp_base

    return (round(long_sl, 5),  long_risk,  round(long_tp, 5),  sl_src_bull,
            round(short_sl, 5), short_risk, round(short_tp, 5), sl_src_bear)


# ═══════════════════════════════════════════
# SCALP RR VALIDATION
# ═══════════════════════════════════════════
def check_scalp_rr_ok(close: float, tp: float, sl: float, tier: str) -> bool:
    """
    Block scalp signals where the reward:risk is below minimum (1.5).
    Non-scalp tiers always pass this check.

    Args:
        close: Entry price
        tp:    Take profit price
        sl:    Stop loss price
        tier:  Signal tier string

    Returns:
        True if signal should fire, False if scalp should be blocked.
    """
    if tier != "SCALP":
        return True   # only scalps are subject to minimum RR check
    risk = abs(close - sl)
    if risk <= 0:
        return False
    rr = abs(tp - close) / risk
    return rr >= SCALP_MIN_RR


# ═══════════════════════════════════════════
# PLACE TRADE ON MT5
# ═══════════════════════════════════════════
def place_trade(
    signal: str,
    sl_price: float,
    tp_price: float,
    lot_size: float,
    symbol: str = None,
    tf_name: str = "30M",
) -> None:
    """
    Submit a market order to MT5 with SL and TP set.
    Sends Telegram confirmation on success, error alert on failure.

    Args:
        signal:    'BUY' or 'SELL'
        sl_price:  Stop loss price
        tp_price:  Take profit price
        lot_size:  Calculated lot size
        symbol:    Trading symbol (defaults to XAUUSD)
        tf_name:   Timeframe string for labelling
    """
    sym  = symbol or SYMBOL
    tick = mt5.symbol_info_tick(sym)   # get current bid/ask prices
    if tick is None:
        send_telegram(f"❌ Failed to get price for {sym}!")
        return

    price      = tick.ask if signal == "BUY" else tick.bid   # use ask for buys, bid for sells
    order_type = mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL
    filling    = get_filling_mode(sym)

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,    # market execution
        "symbol":       sym,
        "volume":       lot_size,
        "type":         order_type,
        "price":        price,
        "sl":           round(sl_price, 5),
        "tp":           round(tp_price, 5),
        "deviation":    20,                        # max slippage in points
        "magic":        234000,                    # magic number to identify bot's trades
        "comment":      f"Mannys V3 {tf_name}",
        "type_time":    mt5.ORDER_TIME_GTC,        # Good Till Cancelled
        "type_filling": filling,
    }

    result  = mt5.order_send(req)
    account = mt5.account_info()

    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        err = result.comment if result else "No response from MT5"
        send_telegram(f"❌ <b>TRADE FAILED</b>\n{sym} {tf_name} {signal}\nError: {err}\nRetcode: {result.retcode if result else 'N/A'}")
        print(f"❌ Trade failed: {err}")
    else:
        dst_str = get_dst_str()
        send_telegram(
            f"🥇 <b>TRADE PLACED — Manny's V3</b>\n\n"
            f"📊 {signal} | {sym} | {tf_name}\n"
            f"🏦 {get_session()} | {dst_str}\n"
            f"📈 Entry: {result.price}\n"
            f"🛑 SL: {round(sl_price,5)}\n"
            f"🎯 TP: {round(tp_price,5)}\n"
            f"📦 Lots: {lot_size}\n"
            f"💼 Balance: {account.balance} GBP\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
        )
        print(f"✅ {sym} {tf_name} {signal} | Entry:{result.price} SL:{round(sl_price,5)} TP:{round(tp_price,5)} Lots:{lot_size}")


# ═══════════════════════════════════════════
# MAIN SIGNAL CHECK
# ═══════════════════════════════════════════
def check_signal(
    last_signal_bar,
    last_bull_bar,
    last_bear_bar,
    symbol: str = None,
    timeframe: int = None,
    ob_state: dict = None,
) -> tuple:
    """
    Core signal evaluation function.
    Runs all checks (bias, structure, triggers, bonus) and applies confidence gating.

    Confidence gate logic (PRIORITY 1):
        score >= session_threshold → auto-execute trade + log
        score 5-6                 → send manual Telegram alert + log (no auto trade)
        score < 5                 → silently ignored (logged as 'ignored')

    Args:
        last_signal_bar: Bar index of most recent signal (any direction)
        last_bull_bar:   Bar index of most recent bull signal
        last_bear_bar:   Bar index of most recent bear signal
        symbol:          Symbol to scan
        timeframe:       MT5 timeframe constant
        ob_state:        Persistent OB state dictionary

    Returns:
        Tuple: (current_bar, new_bull_bar, new_bear_bar)
    """
    sym     = symbol or SYMBOL
    tf      = timeframe or mt5.TIMEFRAME_M30
    tf_name = TIMEFRAME_NAMES.get(tf, "30M")
    key     = f"{sym}_{tf_name}"   # unique key for this symbol+timeframe combo

    if ob_state is None:
        ob_state = {}
    if key not in ob_state:
        ob_state[key] = {'bull_ob': None, 'bear_ob': None}

    # ── GATE: NEWS BLACKOUT ──
    # Must check first — if news is active, skip all processing
    news_black, news_reason = is_news_blackout()
    if news_black:
        print(f"  🚫 {sym} {tf_name} — NEWS BLACKOUT: {news_reason}")
        return last_signal_bar, last_bull_bar, last_bear_bar

    # ── FETCH DATA ──
    df_main = get_candles(tf, 500, sym)
    df_4h   = get_candles(TIMEFRAME_4H, 500, sym)
    df_1h   = get_candles(TIMEFRAME_1H, 500, sym)

    if df_main is None or df_4h is None or df_1h is None:
        print(f"  ⚠ No data: {sym} {tf_name}")
        return last_signal_bar, last_bull_bar, last_bear_bar

    # Need enough bars for all calculations
    if len(df_main) < 210 or len(df_4h) < 18:
        print(f"  ⚠ Not enough bars: {sym} {tf_name}")
        return last_signal_bar, last_bull_bar, last_bear_bar

    # ── INDICATORS ──
    df_main['ema200'] = calc_ema(df_main['close'], 200)
    df_main['ema50']  = calc_ema(df_main['close'], 50)
    df_main['rsi']    = calc_rsi(df_main['close'], 14)
    df_main['atr']    = calc_atr(df_main, ATR_PERIOD)
    df_main['adx']    = calc_adx(df_main, ADX_PERIOD)
    df_main['macd'], df_main['macd_sig'] = calc_macd(df_main['close'])

    last        = df_main.iloc[-2]    # last FULLY CLOSED candle (not current forming one)
    current_bar = last['time']
    bar_index   = len(df_main) - 2

    # Cooldown: prevent firing multiple signals too close together on same symbol/TF
    tf_cooldown = {
        mt5.TIMEFRAME_M5:  12,
        mt5.TIMEFRAME_M15: 10,
        mt5.TIMEFRAME_M30: 8,
        mt5.TIMEFRAME_H1:  5,
        mt5.TIMEFRAME_H4:  3,
    }.get(tf, 8)

    bars_since_bull = (bar_index - last_bull_bar) if last_bull_bar is not None else 999
    bars_since_bear = (bar_index - last_bear_bar) if last_bear_bar is not None else 999
    bull_cooled     = bars_since_bull > tf_cooldown
    bear_cooled     = bars_since_bear > tf_cooldown

    # ── CORE MARKET DATA ──
    close   = last['close']
    ema200  = last['ema200']
    ema50   = last['ema50']
    rsi     = last['rsi']
    atr     = last['atr']
    adx_val = last['adx']
    macd_l  = last['macd']
    macd_s  = last['macd_sig']

    above_e200 = close > ema200
    below_e200 = close < ema200
    pct_below  = (ema200 - close) / ema200 * 100 if ema200 > 0 else 0
    ext_bear   = pct_below > 20    # price is more than 20% below EMA200 — extreme bear
    trending   = adx_val >= ADX_THRESH
    macd_bull  = macd_l > macd_s
    macd_bear  = macd_l < macd_s
    rsi_bull_x = rsi < 40    # RSI oversold — bullish extreme
    rsi_bear_x = rsi > 60    # RSI overbought — bearish extreme

    # ── BIAS DETECTION ──
    wb_high, wb_low, wk_bull, wk_bear, price_in_weekly_range, wk_partial, wk_full = \
        get_weekly_bias(df_4h, sym)
    db_high, db_low, day_bull, day_bear = get_daily_bias(df_4h, sym)   # PRIORITY 2: no fallback
    htf_bull, htf_bear = get_htf_ema(df_1h, df_4h)

    # ── MIXED BIAS TIERS ──
    bias_med_bull = wk_bear and day_bull    # weekly bear + daily bull = medium long
    bias_med_bear = wk_bull and day_bear    # weekly bull + daily bear = medium short
    bias_sca_bull = price_in_weekly_range and day_bull   # inside range + daily bull = scalp long
    bias_sca_bear = price_in_weekly_range and day_bear   # inside range + daily bear = scalp short

    # ── GATE LOGIC (matching Pine Script exactly) ──
    macro_trend_bull = wk_bull and day_bull   # both timeframes bullish = strong alignment
    macro_trend_bear = wk_bear and day_bear

    g_str_bull = wk_bull and day_bull and htf_bull and (not ext_bear) and macro_trend_bull
    g_str_bear = wk_bear and day_bear and htf_bear and (not ext_bear) and macro_trend_bear
    g_med_bull = wk_bear and day_bull and htf_bull and (not ext_bear)
    g_med_bear = wk_bull and day_bear and htf_bear and (not ext_bear)
    g_sca_bull = price_in_weekly_range and day_bull and htf_bull and (not ext_bear)
    g_sca_bear = price_in_weekly_range and day_bear and htf_bear and (not ext_bear)

    # ── PREVIOUS DAY/WEEK/MONTH LEVELS ──
    d1h, d1l      = get_prev_day_hl(df_main)
    w_high, w_low = get_prev_week_hl(df_main)
    m_high, m_low = get_prev_month_hl(df_main)

    w_mid   = (w_low + (w_high - w_low) / 2) if w_high and w_low else None
    in_disc = (close < w_mid) if w_mid else False    # price in discount zone (below midpoint)
    in_prem = (close > w_mid) if w_mid else False    # price in premium zone (above midpoint)
    m_bull  = (close > m_low  and above_e200) if m_low  else False
    m_bear  = (close < m_high and below_e200) if m_high else False

    # ── STRUCTURE CHECKS ──
    # PRIORITY 3: Pass timeframe to check_bos so it uses correct lookback
    rec_bull_bos, rec_bear_bos = check_bos(df_main, df_main['atr'], tf)

    (bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej,
     last_bull_sw_low, last_bear_sw_high) = check_sweeps(df_main, df_main['atr'], d1h, d1l)

    int_bull, int_bear = check_internal_sweep(df_main, above_e200, below_e200)
    eqh_swept, eql_swept = check_eql_swept(df_main, atr)

    # ── TIER 2 STRUCTURE SCORE (0-5) ──
    s2_long  = sum([above_e200, rec_bull_bos, bull_sweep, in_disc, m_bull])
    s2_short = sum([below_e200, rec_bear_bos, bear_sweep, in_prem, m_bear])

    # ── TIER 3 TRIGGERS ──
    bull_fvg_ez, bear_fvg_ez, bull_fvg_bot, bear_fvg_top = check_fvg(
        df_main, df_main['atr'],
        above_e200, below_e200,
        wk_bull, wk_bear, day_bull, day_bear,
        bias_med_bull, bias_med_bear, bias_sca_bull, bias_sca_bear
    )

    # PRIORITY 4: OB detection uses corrected iloc[-5] internally
    bull_ob_rsp, bear_ob_rsp, bull_ob_bot, bear_ob_top = check_ob_stateful(
        df_main, df_main['atr'], above_e200, below_e200, ob_state, key
    )

    # RSI divergence (bonus tier)
    s_bull_div, h_bull_div, s_bear_div, h_bear_div = check_rsi_div(df_main, df_main['rsi'])

    # Pin bar triggers
    bull_wick = last['low']  - min(last['open'], last['close'])
    bear_wick = max(last['open'], last['close']) - last['high']
    pin_body  = abs(last['close'] - last['open'])

    t1_bull = bull_fvg_ez
    t1_bear = bear_fvg_ez
    t2_bull = bull_ob_rsp
    t2_bear = bear_ob_rsp
    t3_bull = bull_sw_rej
    t3_bear = bear_sw_rej
    t4_bull = ((bull_wick >= 2.5 * pin_body) and (pin_body >= atr * 0.1) and
               above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull)
    t4_bear = ((bear_wick >= 2.5 * pin_body) and (pin_body >= atr * 0.1) and
               below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear)
    t5_bull = ((d1h is not None) and (close > d1h) and
               (df_main['close'].iloc[-3] <= d1h) and above_e200 and
               (wk_bull or bias_med_bull or bias_sca_bull) and day_bull)
    t5_bear = ((d1l is not None) and (close < d1l) and
               (df_main['close'].iloc[-3] >= d1l) and below_e200 and
               (wk_bear or bias_med_bear or bias_sca_bear) and day_bear)
    lh1 = (df_main['high'].iloc[-2] < df_main['high'].iloc[-3] and
           df_main['high'].iloc[-3] < df_main['high'].iloc[-4] and
           df_main['high'].iloc[-4] < df_main['high'].iloc[-5])
    hl1 = (df_main['low'].iloc[-2]  > df_main['low'].iloc[-3]  and
           df_main['low'].iloc[-3]  > df_main['low'].iloc[-4]  and
           df_main['low'].iloc[-4]  > df_main['low'].iloc[-5])
    t7_bull = (lh1 and (last['high'] > df_main['high'].iloc[-3]) and
               above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull)
    t7_bear = (hl1 and (last['low']  < df_main['low'].iloc[-3])  and
               below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear)

    any_trig_bull = t1_bull or t2_bull or t3_bull or t4_bull or t5_bull or t7_bull
    any_trig_bear = t1_bear or t2_bear or t3_bear or t4_bear or t5_bear or t7_bear

    trig_name_bull = ("FVG" if t1_bull else "OB" if t2_bull else "Sweep" if t3_bull else
                      "Pin" if t4_bull else "PDH" if t5_bull else "CHoCH" if t7_bull else "None")
    trig_name_bear = ("FVG" if t1_bear else "OB" if t2_bear else "Sweep" if t3_bear else
                      "Pin" if t4_bear else "PDL" if t5_bear else "CHoCH" if t7_bear else "None")

    # ── TIER 4 BONUS SCORE (0-7) ──
    bon_l  = 0
    bon_l += 2 if s_bull_div else (1 if h_bull_div else 0)   # RSI divergence worth most
    bon_l += 1 if macd_bull  else 0
    bon_l += 1 if rsi_bull_x else 0
    bon_l += 1 if trending   else 0
    bon_l += 1 if int_bull   else 0
    bon_l += 1 if eql_swept  else 0

    bon_s  = 0
    bon_s += 2 if s_bear_div else (1 if h_bear_div else 0)
    bon_s += 1 if macd_bear  else 0
    bon_s += 1 if rsi_bear_x else 0
    bon_s += 1 if trending   else 0
    bon_s += 1 if int_bear   else 0
    bon_s += 1 if eqh_swept  else 0

    min_bonus = 2   # minimum bonus score required for signal to pass
    min_str2  = 2   # minimum structure score required

    # ── RAW SIGNAL FIRING (matching Pine Script tier priority) ──
    raw_bull_str = (g_str_bull and bull_cooled and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= min_bonus))
    raw_bear_str = (g_str_bear and bear_cooled and (s2_short >= min_str2) and any_trig_bear and (bon_s >= min_bonus))
    raw_bull_med = (g_med_bull and bull_cooled and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= max(min_bonus-1,0)))
    raw_bear_med = (g_med_bear and bear_cooled and (s2_short >= min_str2) and any_trig_bear and (bon_s >= max(min_bonus-1,0)))
    raw_bull_sca = (g_sca_bull and bull_cooled and (s2_long  >= max(min_str2-1,1)) and any_trig_bull and (bon_l >= max(min_bonus-1,0)))
    raw_bear_sca = (g_sca_bear and bear_cooled and (s2_short >= max(min_str2-1,1)) and any_trig_bear and (bon_s >= max(min_bonus-1,0)))

    # Priority: STRONG > MEDIUM > SCALP
    raw_bull = (raw_bull_str or
                (raw_bull_med and not raw_bull_str) or
                (raw_bull_sca and not raw_bull_str and not raw_bull_med))
    raw_bear = (raw_bear_str or
                (raw_bear_med and not raw_bear_str) or
                (raw_bear_sca and not raw_bear_str and not raw_bear_med))

    bull_tier = "STRONG" if raw_bull_str else ("MEDIUM" if raw_bull_med else ("SCALP" if raw_bull_sca else ""))
    bear_tier = "STRONG" if raw_bear_str else ("MEDIUM" if raw_bear_med else ("SCALP" if raw_bear_sca else ""))

    # ── SL / TP CALCULATION ──
    (long_sl, long_risk, long_tp, sl_src_bull,
     short_sl, short_risk, short_tp, sl_src_bear) = calc_structure_sl_tp(
        close, atr,
        t1_bull, t2_bull, t1_bear, t2_bear,
        bull_ob_bot, bear_ob_top,
        bull_fvg_bot, bear_fvg_top,
        df_main, wb_high, wb_low,
        bull_tier, bear_tier, rr=RR
    )

    # Block scalp if RR < 1.5
    bull_sig_final = raw_bull and check_scalp_rr_ok(close, long_tp,  long_sl,  bull_tier)
    bear_sig_final = raw_bear and check_scalp_rr_ok(close, short_tp, short_sl, bear_tier)

    # ── LOT SIZING ──
    account  = mt5.account_info()
    balance  = account.balance
    risk_amt = balance * RISK_PERCENT
    long_sz  = calc_lot_size(sym, risk_amt, long_risk)
    short_sz = calc_lot_size(sym, risk_amt, short_risk)

    # ── SESSION INFO ──
    session    = get_session()
    dst_str    = get_dst_str()
    bias_h1, bias_h2 = get_pair_bias_hours(sym)
    active_session   = session != "Asian"

    # ── PRIORITY 6: GET SESSION THRESHOLD ──
    # Post-NY requires score 8. All other sessions require score 7.
    session_threshold = get_session_threshold(session)

    # ── PRIORITY 1: CALCULATE CONFIDENCE SCORE ──
    # Calculate separately for bull and bear signals
    conf_score_bull = calc_confidence_score(
        wk_bull, wk_bear, day_bull, day_bear,
        htf_bull, htf_bear, active_session, ext_bear,
        s2_long, s2_short, any_trig_bull, any_trig_bear,
        bon_l, bon_s, min_str2, min_bonus,
        is_bull_signal=True
    )
    conf_score_bear = calc_confidence_score(
        wk_bull, wk_bear, day_bull, day_bear,
        htf_bull, htf_bear, active_session, ext_bear,
        s2_long, s2_short, any_trig_bull, any_trig_bear,
        bon_l, bon_s, min_str2, min_bonus,
        is_bull_signal=False
    )

    # ── STATUS PRINT ──
    wk_status  = ("BULL" if wk_bull else "BEAR" if wk_bear else
                  "PARTIAL(C1 only)" if wk_partial else "UNCONFIRMED")
    day_status = "BULL" if day_bull else "BEAR" if day_bear else "UNCONFIRMED"

    print(f"\n{'='*55}")
    print(f"📈 {sym} {tf_name} | {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC | {session} | {dst_str}")
    print(f"⚡ WkBias:{wk_status} | DayBias:{day_status}")
    print(f"📊 Close:{close:.5f} EMA200:{ema200:.5f} | RSI:{rsi:.1f} ADX:{adx_val:.1f} ATR:{atr:.5f}")
    print(f"💯 ConfBull:{conf_score_bull}/10 ConfBear:{conf_score_bear}/10 | Threshold:{session_threshold}")
    print(f"🔒 HTFBull:{htf_bull} HTFBear:{htf_bear} ExtBear:{ext_bear}")
    print(f"🏗 S2L:{s2_long}/5 S2S:{s2_short}/5 | BonL:{bon_l}/7 BonS:{bon_s}/7")
    print(f"🎯 Trig — Bull:{trig_name_bull}({any_trig_bull}) Bear:{trig_name_bear}({any_trig_bear})")
    print(f"🚦 Raw — Bull:{raw_bull}({bull_tier}) Bear:{raw_bear}({bear_tier})")
    print(f"✅ Final (pre-conf) — Bull:{bull_sig_final} Bear:{bear_sig_final}")

    # ══════════════════════════════════════════════════════
    # PRIORITY 1 — CONFIDENCE GATE: BULL SIGNAL
    # ──────────────────────────────────────────────────────
    # Gate 1: score >= session threshold → auto-execute
    # Gate 2: score 5-6 → manual Telegram alert only
    # Gate 3: score < 5 → silently ignore (log as 'ignored')
    # ══════════════════════════════════════════════════════
    if bull_sig_final:
        emoji = "🔥" if bull_tier == "STRONG" else "💪" if bull_tier == "MEDIUM" else "⚡"

        if conf_score_bull >= session_threshold:
            # ── AUTO EXECUTE: high confidence signal ──
            print(f"\n{emoji} BUY AUTO — {sym} {tf_name} — {bull_tier} — Score:{conf_score_bull}/10 — Trig:{trig_name_bull}")

            # PRIORITY 5: Log trade BEFORE executing
            log_trade(
                symbol=sym, timeframe=tf_name, tier=bull_tier,
                confidence_score=conf_score_bull, entry_price=close,
                sl_price=long_sl, tp_price=long_tp,
                trigger_type=trig_name_bull, sl_source=sl_src_bull,
                session=session, status="auto-executed"
            )

            send_telegram(
                f"{emoji} <b>{bull_tier} BUY — {sym} {tf_name}</b>\n\n"
                f"📊 Manny's V3 Mixed Bias | Auto-Executed\n"
                f"💯 Confidence: {conf_score_bull}/10 (threshold: {session_threshold})\n"
                f"🎯 Trigger: {trig_name_bull} | SL from: {sl_src_bull}\n"
                f"🧭 Weekly: {wk_status} | Daily: {day_status}\n"
                f"🏦 {session} | {dst_str}\n"
                f"📈 Entry: {close:.5f}\n"
                f"🛑 SL: {long_sl}\n"
                f"🎯 TP: {long_tp}\n"
                f"📦 Lots: {long_sz}\n"
                f"🏗 S2:{s2_long}/5 Bonus:{bon_l}/7\n"
                f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
            )
            place_trade("BUY", long_sl, long_tp, long_sz, sym, tf_name)
            return current_bar, bar_index, last_bear_bar

        elif conf_score_bull >= CONFIDENCE_MANUAL_MIN:
            # ── MANUAL ALERT: moderate confidence, let user decide ──
            print(f"\n⚠️ BUY MANUAL ALERT — {sym} {tf_name} — Score:{conf_score_bull}/10 (below threshold {session_threshold})")

            # PRIORITY 5: Log as manual alert
            log_trade(
                symbol=sym, timeframe=tf_name, tier=bull_tier,
                confidence_score=conf_score_bull, entry_price=close,
                sl_price=long_sl, tp_price=long_tp,
                trigger_type=trig_name_bull, sl_source=sl_src_bull,
                session=session, status="manual-alert"
            )

            send_telegram(
                f"⚠️ <b>MANUAL REVIEW — BUY {sym} {tf_name}</b>\n\n"
                f"📊 Manny's V3 | Low Confidence — Manual Entry Only\n"
                f"💯 Confidence: {conf_score_bull}/10 (need {session_threshold} for auto)\n"
                f"🎯 Trigger: {trig_name_bull} | SL from: {sl_src_bull}\n"
                f"🧭 Weekly: {wk_status} | Daily: {day_status}\n"
                f"🏦 {session} | {dst_str}\n"
                f"📈 Suggested Entry: {close:.5f}\n"
                f"🛑 SL: {long_sl}\n"
                f"🎯 TP: {long_tp}\n"
                f"📦 Suggested Lots: {long_sz}\n"
                f"⚠️ Enter manually only if confluence looks strong to you\n"
                f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
            )
            # No place_trade() call — user decides manually
            return current_bar, bar_index, last_bear_bar

        else:
            # ── IGNORED: confidence too low ──
            print(f"  🔕 BUY ignored — Score:{conf_score_bull}/10 (below minimum {CONFIDENCE_MANUAL_MIN})")
            # PRIORITY 5: Still log it so you can see what was filtered out
            log_trade(
                symbol=sym, timeframe=tf_name, tier=bull_tier,
                confidence_score=conf_score_bull, entry_price=close,
                sl_price=long_sl, tp_price=long_tp,
                trigger_type=trig_name_bull, sl_source=sl_src_bull,
                session=session, status="ignored"
            )

    # ══════════════════════════════════════════════════════
    # PRIORITY 1 — CONFIDENCE GATE: BEAR SIGNAL
    # Same three-tier gate as bull signal above
    # ══════════════════════════════════════════════════════
    elif bear_sig_final:
        emoji = "🔥" if bear_tier == "STRONG" else "💪" if bear_tier == "MEDIUM" else "⚡"

        if conf_score_bear >= session_threshold:
            # ── AUTO EXECUTE ──
            print(f"\n{emoji} SELL AUTO — {sym} {tf_name} — {bear_tier} — Score:{conf_score_bear}/10 — Trig:{trig_name_bear}")

            # PRIORITY 5: Log trade BEFORE executing
            log_trade(
                symbol=sym, timeframe=tf_name, tier=bear_tier,
                confidence_score=conf_score_bear, entry_price=close,
                sl_price=short_sl, tp_price=short_tp,
                trigger_type=trig_name_bear, sl_source=sl_src_bear,
                session=session, status="auto-executed"
            )

            send_telegram(
                f"{emoji} <b>{bear_tier} SELL — {sym} {tf_name}</b>\n\n"
                f"📊 Manny's V3 Mixed Bias | Auto-Executed\n"
                f"💯 Confidence: {conf_score_bear}/10 (threshold: {session_threshold})\n"
                f"🎯 Trigger: {trig_name_bear} | SL from: {sl_src_bear}\n"
                f"🧭 Weekly: {wk_status} | Daily: {day_status}\n"
                f"🏦 {session} | {dst_str}\n"
                f"📈 Entry: {close:.5f}\n"
                f"🛑 SL: {short_sl}\n"
                f"🎯 TP: {short_tp}\n"
                f"📦 Lots: {short_sz}\n"
                f"🏗 S2:{s2_short}/5 Bonus:{bon_s}/7\n"
                f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
            )
            place_trade("SELL", short_sl, short_tp, short_sz, sym, tf_name)
            return current_bar, last_bull_bar, bar_index

        elif conf_score_bear >= CONFIDENCE_MANUAL_MIN:
            # ── MANUAL ALERT ──
            print(f"\n⚠️ SELL MANUAL ALERT — {sym} {tf_name} — Score:{conf_score_bear}/10 (below threshold {session_threshold})")

            log_trade(
                symbol=sym, timeframe=tf_name, tier=bear_tier,
                confidence_score=conf_score_bear, entry_price=close,
                sl_price=short_sl, tp_price=short_tp,
                trigger_type=trig_name_bear, sl_source=sl_src_bear,
                session=session, status="manual-alert"
            )

            send_telegram(
                f"⚠️ <b>MANUAL REVIEW — SELL {sym} {tf_name}</b>\n\n"
                f"📊 Manny's V3 | Low Confidence — Manual Entry Only\n"
                f"💯 Confidence: {conf_score_bear}/10 (need {session_threshold} for auto)\n"
                f"🎯 Trigger: {trig_name_bear} | SL from: {sl_src_bear}\n"
                f"🧭 Weekly: {wk_status} | Daily: {day_status}\n"
                f"🏦 {session} | {dst_str}\n"
                f"📈 Suggested Entry: {close:.5f}\n"
                f"🛑 SL: {short_sl}\n"
                f"🎯 TP: {short_tp}\n"
                f"📦 Suggested Lots: {short_sz}\n"
                f"⚠️ Enter manually only if confluence looks strong to you\n"
                f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
            )
            return current_bar, last_bull_bar, bar_index

        else:
            # ── IGNORED ──
            print(f"  🔕 SELL ignored — Score:{conf_score_bear}/10 (below minimum {CONFIDENCE_MANUAL_MIN})")
            log_trade(
                symbol=sym, timeframe=tf_name, tier=bear_tier,
                confidence_score=conf_score_bear, entry_price=close,
                sl_price=short_sl, tp_price=short_tp,
                trigger_type=trig_name_bear, sl_source=sl_src_bear,
                session=session, status="ignored"
            )

    return current_bar, last_bull_bar, last_bear_bar


# ═══════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════
def run() -> None:
    """
    Main entry point. Connects to MT5 and runs the scanner loop continuously.
    Scans all symbols and timeframes during active sessions.
    Sleeps during Asian session to save resources.
    """
    print("🚀 Manny's Gold Strategy V3 — Mixed Bias Edition (Optimized)")
    print("=" * 55)
    print("✅ PRIORITY 1: Confidence gate active (7+ auto | 5-6 manual | <5 ignored)")
    print("✅ PRIORITY 2: Daily bias fallback removed — always midnight UTC candle")
    print("✅ PRIORITY 3: BOS reset timeframe-aware (50/30/20 bars)")
    print("✅ PRIORITY 4: OB candle index corrected (iloc[-5] not iloc[-6])")
    print("✅ PRIORITY 5: Trade logging to trade_log.csv active")
    print("✅ PRIORITY 6: Session thresholds (Prime=7, Post-NY=8)")
    print("=" * 55)

    h1, h2  = get_pair_bias_hours("XAUUSD")
    dst     = get_dst_str()
    print(f"🕐 {dst} | Gold bias hours: {h1}AM + {h2}AM UTC")
    print(f"📈 Scanning: {', '.join(SYMBOLS)}")

    send_telegram(
        f"🚀 <b>Manny's V3 — Optimized Scanner Started</b>\n\n"
        f"✅ Confidence gate: 7+ auto | 5-6 manual | &lt;5 ignored\n"
        f"✅ Post-NY threshold: 8 (stricter during quiet hours)\n"
        f"✅ Daily bias: always midnight UTC candle (no fallback)\n"
        f"✅ BOS reset: timeframe-aware\n"
        f"✅ OB candle index: corrected\n"
        f"✅ Trade log: trade_log.csv\n"
        f"📈 Pairs: {', '.join(SYMBOLS)}\n"
        f"⏱ Timeframes: 5M 15M 30M 1H 4H\n"
        f"🕐 {dst} | Gold: {h1}AM+{h2}AM UTC"
    )

    if not connect_mt5():
        print("Failed to connect to MT5")
        return

    print("✅ MT5 Connected!")

    # Initialise persistent state for all symbol/timeframe combinations
    ob_state     = {}
    signal_state = {}

    for s in SYMBOLS:
        for tf in SYMBOL_TIMEFRAMES.get(s, [mt5.TIMEFRAME_M30]):
            key = f"{s}_{TIMEFRAME_NAMES[tf]}"
            signal_state[key] = {
                "last_signal_bar": None,
                "last_bull_bar":   None,
                "last_bear_bar":   None
            }
            ob_state[key] = {'bull_ob': None, 'bear_ob': None}

    # ── MAIN SCAN LOOP ──
    while True:
        now = datetime.now(timezone.utc)
        news_black, news_reason = is_news_blackout()

        if is_active_session():
            status = f"🚫 NEWS BLACKOUT: {news_reason}" if news_black else get_session()
            print(f"\n[{now.strftime('%H:%M:%S')} UTC] Scanning | {status}")

            for sym in SYMBOLS:
                for tf in SYMBOL_TIMEFRAMES.get(sym, [mt5.TIMEFRAME_M30]):
                    tf_name = TIMEFRAME_NAMES[tf]
                    key     = f"{sym}_{tf_name}"
                    try:
                        state = signal_state[key]
                        new_sig, new_bull, new_bear = check_signal(
                            state["last_signal_bar"],
                            state["last_bull_bar"],
                            state["last_bear_bar"],
                            symbol=sym,
                            timeframe=tf,
                            ob_state=ob_state
                        )
                        signal_state[key]["last_signal_bar"] = new_sig
                        signal_state[key]["last_bull_bar"]   = new_bull
                        signal_state[key]["last_bear_bar"]   = new_bear
                    except Exception as e:
                        print(f"❌ {sym} {tf_name} Error: {e}")
                        import traceback
                        traceback.print_exc()
                        send_telegram(f"⚠️ {sym} {tf_name} error: {e}")
        else:
            h1, _ = get_pair_bias_hours("XAUUSD")
            print(f"[{now.strftime('%H:%M:%S')} UTC] 💤 Asian — Next: London 07:00 UTC | Gold Bias: {h1}AM UTC")

        time.sleep(CHECK_INTERVAL)   # wait 60 seconds before next scan


if __name__ == "__main__":
    run()
