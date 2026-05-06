"""
dashboard.py — Manny's Gold Strategy V5 — Live Trading Dashboard
=================================================================
Purpose:
    A real-time Streamlit dashboard that connects to MT5 and displays
    live market data, open trades, account health, pair analysis,
    trade journal, and upcoming news events.

    Runs alongside scanner_v4.py — does NOT interfere with trading.
    Auto-refreshes every 60 seconds.

    To run:
        streamlit run dashboard.py

Inputs:
    - MT5 live connection (same .env credentials as scanner)
    - trade_journal.xlsx (auto-reads latest trades)

Outputs:
    - Local browser dashboard at http://localhost:8501

Author: Emmanuel Ogbu (Manny)
Date: May 2026
"""

import os                          # file system and environment variables
import time                        # sleep and timing
import MetaTrader5 as mt5          # MT5 Python API for live data
import pandas as pd                # data manipulation
import numpy as np                 # numerical operations
import streamlit as st             # web dashboard framework
from dotenv import load_dotenv     # load .env credentials
from datetime import datetime, timezone, timedelta  # UTC time handling
from openpyxl import load_workbook # read Excel journal

load_dotenv()   # load credentials from .env

# ═══════════════════════════════════════════
# CREDENTIALS
# ═══════════════════════════════════════════
MT5_LOGIN    = int(os.getenv("MT5_LOGIN"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER   = os.getenv("MT5_SERVER")

# ═══════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════
SYMBOLS = ["XAUUSD", "XAGUSD", "US500", "US30", "BTCUSD", "USTEC", "NZDUSD"]

DAILY_DRAWDOWN_LIMIT = 0.03   # 3% daily drawdown limit

TRADE_JOURNAL_FILE = "trade_journal.xlsx"

# Session hours UTC
SESSIONS = {
    "London":   (7,  12),
    "Overlap":  (12, 13),
    "New York": (13, 17),
    "Post-NY":  (17, 22),
    "Asian":    (22, 7),
}

# High impact news events — matches scanner_v4.py exactly
CPI_DATES = [
    datetime(2026,5,13,13,30,tzinfo=timezone.utc),
    datetime(2026,6,11,13,30,tzinfo=timezone.utc),
    datetime(2026,7,14,13,30,tzinfo=timezone.utc),
    datetime(2026,8,12,13,30,tzinfo=timezone.utc),
    datetime(2026,9,10,13,30,tzinfo=timezone.utc),
    datetime(2026,10,8,13,30,tzinfo=timezone.utc),
    datetime(2026,11,12,13,30,tzinfo=timezone.utc),
    datetime(2026,12,9,13,30,tzinfo=timezone.utc),
]

FOMC_DATES = [
    datetime(2026,5,6,19,0,tzinfo=timezone.utc),
    datetime(2026,6,17,19,0,tzinfo=timezone.utc),
    datetime(2026,7,29,19,0,tzinfo=timezone.utc),
    datetime(2026,9,16,19,0,tzinfo=timezone.utc),
    datetime(2026,11,4,19,0,tzinfo=timezone.utc),
    datetime(2026,12,16,19,0,tzinfo=timezone.utc),
]

NEWS_WINDOW = timedelta(minutes=30)


# ═══════════════════════════════════════════
# PAGE CONFIG — must be first Streamlit call
# ═══════════════════════════════════════════
st.set_page_config(
    page_title="Manny's Gold Bot — Live Dashboard",
    page_icon="🥇",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ═══════════════════════════════════════════
# CUSTOM CSS — dark trading terminal aesthetic
# ═══════════════════════════════════════════
st.markdown("""
<style>
    /* Import fonts */
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;600;700;800&display=swap');

    /* Global dark theme */
    .stApp {
        background-color: #0a0e1a;
        color: #e2e8f0;
        font-family: 'JetBrains Mono', monospace;
    }

    /* Hide default streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {padding-top: 1rem; padding-bottom: 1rem;}

    /* Main title */
    .dashboard-title {
        font-family: 'Syne', sans-serif;
        font-size: 2rem;
        font-weight: 800;
        color: #f6c90e;
        letter-spacing: -0.02em;
        margin-bottom: 0;
    }

    .dashboard-subtitle {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        color: #64748b;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin-top: 0.2rem;
    }

    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #0f1629 0%, #131d35 100%);
        border: 1px solid #1e2d4a;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.8rem;
    }

    .metric-label {
        font-size: 0.65rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        margin-bottom: 0.3rem;
    }

    .metric-value {
        font-family: 'Syne', sans-serif;
        font-size: 1.8rem;
        font-weight: 700;
        color: #f6c90e;
    }

    .metric-value-green { color: #22c55e; }
    .metric-value-red   { color: #ef4444; }
    .metric-value-white { color: #e2e8f0; }

    /* Pair cards */
    .pair-card {
        background: linear-gradient(135deg, #0f1629 0%, #131d35 100%);
        border: 1px solid #1e2d4a;
        border-radius: 12px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.6rem;
        transition: border-color 0.2s;
    }

    .pair-card-bull { border-left: 3px solid #22c55e; }
    .pair-card-bear { border-left: 3px solid #ef4444; }
    .pair-card-neutral { border-left: 3px solid #64748b; }

    .pair-name {
        font-family: 'Syne', sans-serif;
        font-size: 1rem;
        font-weight: 700;
        color: #f6c90e;
    }

    .pair-meta {
        font-size: 0.7rem;
        color: #94a3b8;
        margin-top: 0.2rem;
    }

    /* Bias badges */
    .badge {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        border-radius: 4px;
        font-size: 0.65rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-right: 0.3rem;
    }

    .badge-bull { background: #14532d; color: #22c55e; }
    .badge-bear { background: #450a0a; color: #ef4444; }
    .badge-neutral { background: #1e293b; color: #64748b; }
    .badge-gold { background: #422006; color: #f6c90e; }
    .badge-blue { background: #0c1a3a; color: #60a5fa; }

    /* Section headers */
    .section-header {
        font-family: 'Syne', sans-serif;
        font-size: 0.7rem;
        font-weight: 700;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.2em;
        margin-bottom: 0.8rem;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid #1e2d4a;
    }

    /* Trade rows */
    .trade-row-profit { background: rgba(34,197,94,0.08); border-radius: 6px; padding: 0.5rem; margin-bottom: 0.3rem; }
    .trade-row-loss   { background: rgba(239,68,68,0.08);  border-radius: 6px; padding: 0.5rem; margin-bottom: 0.3rem; }
    .trade-row-open   { background: rgba(246,201,14,0.08); border-radius: 6px; padding: 0.5rem; margin-bottom: 0.3rem; }

    /* News card */
    .news-card {
        background: #0f1629;
        border: 1px solid #1e2d4a;
        border-radius: 8px;
        padding: 0.8rem 1rem;
        margin-bottom: 0.5rem;
    }

    .news-high   { border-left: 3px solid #ef4444; }
    .news-medium { border-left: 3px solid #f97316; }

    /* Drawdown bar */
    .dd-bar-container {
        background: #1e2d4a;
        border-radius: 6px;
        height: 8px;
        width: 100%;
        margin-top: 0.5rem;
    }

    /* Session pill */
    .session-pill {
        display: inline-block;
        background: #f6c90e;
        color: #0a0e1a;
        font-weight: 700;
        font-size: 0.7rem;
        padding: 0.2rem 0.8rem;
        border-radius: 20px;
        letter-spacing: 0.05em;
    }

    /* Divider */
    hr { border-color: #1e2d4a; }

    /* Streamlit metric override */
    [data-testid="metric-container"] {
        background: #0f1629;
        border: 1px solid #1e2d4a;
        border-radius: 12px;
        padding: 1rem;
    }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════
# MT5 CONNECTION
# ═══════════════════════════════════════════
@st.cache_resource
def connect_mt5():
    """
    Connect to MT5 once and cache the connection.
    st.cache_resource means this only runs once per session.

    Returns:
        True if connected, False if failed.
    """
    if not mt5.initialize(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
        return False
    return True


# ═══════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════
def get_session() -> str:
    """Return current trading session name."""
    h = datetime.now(timezone.utc).hour
    if   7  <= h < 12: return "London"
    elif 12 <= h < 13: return "Overlap"
    elif 13 <= h < 17: return "New York"
    elif 17 <= h < 22: return "Post-NY"
    else:              return "Asian"


def get_account_info() -> dict:
    """
    Fetch live account information from MT5.

    Returns:
        Dictionary with balance, equity, profit, margin fields.
    """
    account = mt5.account_info()
    if account is None:
        return {"balance": 0, "equity": 0, "profit": 0, "margin": 0, "currency": "GBP"}
    return {
        "balance":  account.balance,
        "equity":   account.equity,
        "profit":   account.profit,   # floating P&L on open trades
        "margin":   account.margin,
        "currency": account.currency,
    }


def get_open_trades() -> list:
    """
    Fetch all open positions placed by this bot (magic 234000).

    Returns:
        List of position dictionaries with trade details.
    """
    positions = mt5.positions_get()
    if positions is None:
        return []

    trades = []
    for pos in positions:
        if pos.magic != 234000:
            continue   # only show bot trades

        direction = "BUY" if pos.type == mt5.ORDER_TYPE_BUY else "SELL"
        risk      = abs(pos.price_open - pos.sl) if pos.sl > 0 else 1
        progress  = (pos.price_current - pos.price_open) if direction == "BUY" else (pos.price_open - pos.price_current)
        rr_now    = progress / risk if risk > 0 else 0

        trades.append({
            "ticket":    pos.ticket,
            "symbol":    pos.symbol,
            "direction": direction,
            "entry":     pos.price_open,
            "current":   pos.price_current,
            "sl":        pos.sl,
            "tp":        pos.tp,
            "volume":    pos.volume,
            "profit":    pos.profit,
            "rr_now":    rr_now,
        })
    return trades


def get_candles(symbol: str, timeframe: int, count: int = 200) -> pd.DataFrame:
    """Fetch OHLCV candles from MT5 as DataFrame."""
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    return df


def calc_ema(series: pd.Series, period: int) -> pd.Series:
    """Calculate EMA matching TradingView exactly."""
    return series.ewm(span=period, adjust=False).mean()


def get_pair_analysis(symbol: str) -> dict:
    """
    Analyse a single pair for bias, EMA, session status, and last price.

    Returns dictionary with all display data for the pair card.
    """
    # Fetch 4H data for bias detection
    df_4h = get_candles(symbol, mt5.TIMEFRAME_H4, 100)
    if df_4h is None or len(df_4h) < 10:
        return {"symbol": symbol, "error": True}

    close    = df_4h['close'].iloc[-1]
    ema200   = calc_ema(df_4h['close'], 200).iloc[-1]
    ema50    = calc_ema(df_4h['close'], 50).iloc[-1]

    # Simple bias from EMA position
    above_ema = close > ema200
    below_ema = close < ema200

    # ATR for volatility reading
    hl = df_4h['high'] - df_4h['low']
    atr = hl.ewm(span=14).mean().iloc[-1]

    # Price change last 4H candle
    prev_close = df_4h['close'].iloc[-2]
    change_pct = ((close - prev_close) / prev_close) * 100

    # Weekly bias: simple check — is price above last week's open?
    week_open = df_4h['open'].iloc[-42] if len(df_4h) >= 42 else df_4h['open'].iloc[0]
    wk_bull = close > week_open
    wk_bear = close < week_open

    # Daily bias: is price above today's open?
    day_open  = df_4h['open'].iloc[-6] if len(df_4h) >= 6 else df_4h['open'].iloc[0]
    day_bull  = close > day_open
    day_bear  = close < day_open

    # Overall direction
    if above_ema and wk_bull and day_bull:
        direction = "BULL"
    elif below_ema and wk_bear and day_bear:
        direction = "BEAR"
    else:
        direction = "MIXED"

    return {
        "symbol":     symbol,
        "close":      close,
        "ema200":     ema200,
        "ema50":      ema50,
        "atr":        atr,
        "above_ema":  above_ema,
        "wk_bull":    wk_bull,
        "wk_bear":    wk_bear,
        "day_bull":   day_bull,
        "day_bear":   day_bear,
        "direction":  direction,
        "change_pct": change_pct,
        "error":      False,
    }


def get_upcoming_news() -> list:
    """
    Check for upcoming high impact news events in the next 24 hours.

    Returns:
        List of news event dicts sorted by time.
    """
    now    = datetime.now(timezone.utc)
    events = []

    # NFP — first Friday of month at 13:30 UTC
    if now.weekday() == 4 and now.day <= 7:
        nfp = now.replace(hour=13, minute=30, second=0, microsecond=0)
        if nfp > now:
            events.append({"name": "NFP", "time": nfp, "impact": "HIGH"})

    # CPI events
    for dt in CPI_DATES:
        if now < dt < now + timedelta(hours=24):
            events.append({"name": "CPI", "time": dt, "impact": "HIGH"})

    # FOMC events
    for dt in FOMC_DATES:
        if now < dt < now + timedelta(hours=24):
            events.append({"name": "FOMC", "time": dt, "impact": "HIGH"})

    # Sort by time
    events.sort(key=lambda x: x["time"])
    return events


def is_news_blackout() -> tuple:
    """Check if currently in news blackout window."""
    now = datetime.now(timezone.utc)
    if now.weekday() == 4 and now.day <= 7:
        nfp = now.replace(hour=13, minute=30, second=0, microsecond=0)
        if abs(now - nfp) <= NEWS_WINDOW:
            return True, "NFP"
    for dt in CPI_DATES:
        if abs(now - dt) <= NEWS_WINDOW:
            return True, "CPI"
    for dt in FOMC_DATES:
        if abs(now - dt) <= NEWS_WINDOW:
            return True, "FOMC"
    return False, ""


def load_journal() -> pd.DataFrame:
    """
    Load the trade journal Excel file into a DataFrame.
    Returns empty DataFrame if file doesn't exist yet.
    """
    if not os.path.isfile(TRADE_JOURNAL_FILE):
        return pd.DataFrame()   # no journal yet — return empty

    try:
        df = pd.read_excel(TRADE_JOURNAL_FILE)
        return df
    except Exception:
        return pd.DataFrame()


def calc_stats(df: pd.DataFrame) -> dict:
    """
    Calculate performance statistics from the trade journal.
    Shows all signal counts plus closed trade P&L stats.

    Args:
        df: Trade journal DataFrame

    Returns:
        Dict with win_rate, profit_factor, total, winners, losers,
        total_pnl, total_signals, auto_executed, manual_alerts
    """
    empty = {
        "win_rate": 0, "profit_factor": 0, "total": 0,
        "winners": 0, "losers": 0, "total_pnl": 0,
        "total_signals": 0, "auto_executed": 0, "manual_alerts": 0
    }

    if df.empty:
        return empty

    # Count all signals regardless of outcome
    total_signals = len(df)
    auto_executed = len(df[df["status"] == "auto-executed"]) if "status" in df.columns else 0
    manual_alerts = len(df[df["status"] == "manual-alert"])  if "status" in df.columns else 0

    if "actual_pnl" not in df.columns:
        empty.update({"total_signals": total_signals, "auto_executed": auto_executed, "manual_alerts": manual_alerts})
        return empty

    # Only analyse closed trades with P&L recorded
    closed = df[df["actual_pnl"].notna() & (df["actual_pnl"] != "")].copy()

    if len(closed) == 0:
        empty.update({"total_signals": total_signals, "auto_executed": auto_executed, "manual_alerts": manual_alerts})
        return empty

    closed["actual_pnl"] = pd.to_numeric(closed["actual_pnl"], errors="coerce")
    closed = closed.dropna(subset=["actual_pnl"])

    winners      = closed[closed["actual_pnl"] > 0]
    losers       = closed[closed["actual_pnl"] < 0]
    total        = len(closed)
    win_rate     = (len(winners) / total * 100) if total > 0 else 0
    gross_profit = winners["actual_pnl"].sum() if len(winners) > 0 else 0
    gross_loss   = abs(losers["actual_pnl"].sum()) if len(losers) > 0 else 0
    pf           = (gross_profit / gross_loss) if gross_loss > 0 else 0

    return {
        "win_rate":      round(win_rate, 1),
        "profit_factor": round(pf, 2),
        "total":         total,
        "winners":       len(winners),
        "losers":        len(losers),
        "total_pnl":     round(closed["actual_pnl"].sum(), 2),
        "total_signals": total_signals,
        "auto_executed": auto_executed,
        "manual_alerts": manual_alerts,
    }


# ═══════════════════════════════════════════
# MAIN DASHBOARD RENDER
# ═══════════════════════════════════════════
def render_dashboard():
    """
    Main function that renders the entire dashboard.
    Called on every refresh cycle.
    """
    now     = datetime.now(timezone.utc)
    session = get_session()
    blackout, blackout_reason = is_news_blackout()

    # ── HEADER ──
    col_title, col_time, col_session = st.columns([3, 2, 1])

    with col_title:
        st.markdown('<div class="dashboard-title">🥇 MANNY\'S GOLD BOT</div>', unsafe_allow_html=True)
        st.markdown('<div class="dashboard-subtitle">V4 Live | Smart Money Concepts | Mixed Bias Edition</div>', unsafe_allow_html=True)

    with col_time:
        st.markdown(f'<div style="text-align:right; padding-top:0.5rem;">'
                    f'<div class="metric-label">UTC Time</div>'
                    f'<div style="font-family:\'JetBrains Mono\'; font-size:1.2rem; color:#e2e8f0;">'
                    f'{now.strftime("%H:%M:%S")}</div>'
                    f'<div class="metric-label">{now.strftime("%A %d %B %Y")}</div>'
                    f'</div>', unsafe_allow_html=True)

    with col_session:
        session_color = {"London": "#22c55e", "Overlap": "#f59e0b", "New York": "#3b82f6", "Post-NY": "#8b5cf6", "Asian": "#64748b"}.get(session, "#64748b")
        st.markdown(f'<div style="text-align:right; padding-top:0.8rem;">'
                    f'<span style="background:{session_color}22; color:{session_color}; border:1px solid {session_color}44; '
                    f'padding:0.3rem 0.8rem; border-radius:20px; font-size:0.75rem; font-weight:700;">'
                    f'{session}</span></div>', unsafe_allow_html=True)

    # News blackout banner
    if blackout:
        st.markdown(f'<div style="background:#450a0a; border:1px solid #ef4444; border-radius:8px; padding:0.6rem 1rem; margin:0.5rem 0; color:#ef4444; font-weight:600;">🚫 NEWS BLACKOUT ACTIVE — {blackout_reason} ±30min — No trades firing</div>', unsafe_allow_html=True)

    st.markdown("---")

    # ── ACCOUNT HEALTH ROW ──
    account = get_account_info()
    open_trades = get_open_trades()

    st.markdown('<div class="section-header">Account Health</div>', unsafe_allow_html=True)

    c1, c2, c3, c4, c5 = st.columns(5)

    with c1:
        st.metric("Balance", f"{account['currency']} {account['balance']:,.2f}")

    with c2:
        equity_delta = account['equity'] - account['balance']
        st.metric("Equity", f"{account['currency']} {account['equity']:,.2f}",
                  delta=f"{equity_delta:+.2f}")

    with c3:
        profit_color = "normal" if account['profit'] >= 0 else "inverse"
        st.metric("Floating P&L", f"{account['currency']} {account['profit']:+.2f}")

    with c4:
        st.metric("Open Trades", len(open_trades))

    with c5:
        # Daily drawdown meter
        daily_loss     = min(account['equity'] - account['balance'], 0)   # negative or zero
        dd_limit       = account['balance'] * DAILY_DRAWDOWN_LIMIT
        dd_pct         = abs(daily_loss / dd_limit * 100) if dd_limit > 0 else 0
        dd_pct_clamped = min(dd_pct, 100)
        dd_color       = "#22c55e" if dd_pct < 50 else "#f97316" if dd_pct < 80 else "#ef4444"
        st.markdown(
            f'<div class="metric-label">Daily Drawdown</div>'
            f'<div style="font-size:1.2rem; font-weight:700; color:{dd_color};">{dd_pct_clamped:.1f}% / 100%</div>'
            f'<div class="dd-bar-container"><div style="height:8px; width:{dd_pct_clamped}%; background:{dd_color}; border-radius:6px; transition:width 0.3s;"></div></div>'
            f'<div class="metric-label" style="margin-top:0.3rem;">Limit: {account["currency"]} {dd_limit:,.0f}</div>',
            unsafe_allow_html=True
        )

    st.markdown("---")

    # ── OPEN TRADES ──
    st.markdown('<div class="section-header">Open Trades</div>', unsafe_allow_html=True)

    if len(open_trades) == 0:
        st.markdown('<div style="color:#64748b; font-size:0.85rem; padding:0.5rem 0;">No open trades right now</div>', unsafe_allow_html=True)
    else:
        for trade in open_trades:
            profit_color = "#22c55e" if trade['profit'] >= 0 else "#ef4444"
            dir_color    = "#22c55e" if trade['direction'] == "BUY" else "#ef4444"
            rr_color     = "#22c55e" if trade['rr_now'] >= 1 else "#f97316" if trade['rr_now'] >= 0 else "#ef4444"

            st.markdown(
                f'<div class="trade-row-open">'
                f'<span style="color:{dir_color}; font-weight:700; margin-right:1rem;">{"▲" if trade["direction"]=="BUY" else "▼"} {trade["direction"]}</span>'
                f'<span style="color:#f6c90e; font-weight:700; margin-right:1rem;">{trade["symbol"]}</span>'
                f'<span style="color:#94a3b8; margin-right:1rem;">Entry: {trade["entry"]:.5f}</span>'
                f'<span style="color:#94a3b8; margin-right:1rem;">Current: {trade["current"]:.5f}</span>'
                f'<span style="color:#94a3b8; margin-right:1rem;">SL: {trade["sl"]:.5f}</span>'
                f'<span style="color:#94a3b8; margin-right:1rem;">TP: {trade["tp"]:.5f}</span>'
                f'<span style="color:{rr_color}; margin-right:1rem;">RR: {trade["rr_now"]:.2f}</span>'
                f'<span style="color:{profit_color}; font-weight:700;">P&L: {trade["profit"]:+.2f}</span>'
                f'</div>',
                unsafe_allow_html=True
            )

    st.markdown("---")

    # ── PAIR ANALYSIS + NEWS (side by side) ──
    left_col, right_col = st.columns([3, 1])

    with left_col:
        st.markdown('<div class="section-header">Pair Analysis</div>', unsafe_allow_html=True)

        # Scan pairs in 3 columns
        pair_cols = st.columns(3)
        for i, symbol in enumerate(SYMBOLS):
            analysis = get_pair_analysis(symbol)
            col = pair_cols[i % 3]

            with col:
                if analysis.get("error"):
                    st.markdown(f'<div class="pair-card pair-card-neutral"><div class="pair-name">{symbol}</div><div class="pair-meta">No data</div></div>', unsafe_allow_html=True)
                    continue

                direction  = analysis["direction"]
                card_class = "pair-card-bull" if direction == "BULL" else "pair-card-bear" if direction == "BEAR" else "pair-card-neutral"
                dir_emoji  = "🟢" if direction == "BULL" else "🔴" if direction == "BEAR" else "🟡"
                change_color = "#22c55e" if analysis["change_pct"] >= 0 else "#ef4444"

                wk_badge  = f'<span class="badge badge-bull">WK BULL</span>' if analysis["wk_bull"] else f'<span class="badge badge-bear">WK BEAR</span>'
                day_badge = f'<span class="badge badge-bull">DAY BULL</span>' if analysis["day_bull"] else f'<span class="badge badge-bear">DAY BEAR</span>'
                ema_badge = f'<span class="badge badge-gold">↑ EMA200</span>' if analysis["above_ema"] else f'<span class="badge badge-bear">↓ EMA200</span>'

                st.markdown(
                    f'<div class="pair-card {card_class}">'
                    f'<div class="pair-name">{dir_emoji} {symbol}</div>'
                    f'<div style="font-size:1.1rem; color:#e2e8f0; margin:0.3rem 0;">{analysis["close"]:.5f} '
                    f'<span style="font-size:0.75rem; color:{change_color};">{analysis["change_pct"]:+.2f}%</span></div>'
                    f'<div style="margin:0.4rem 0;">{wk_badge}{day_badge}</div>'
                    f'<div>{ema_badge}<span class="badge badge-blue">ATR {analysis["atr"]:.2f}</span></div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

    with right_col:
        st.markdown('<div class="section-header">Upcoming News</div>', unsafe_allow_html=True)

        events = get_upcoming_news()
        if len(events) == 0:
            st.markdown('<div style="color:#64748b; font-size:0.8rem;">No high impact events in next 24 hours ✅</div>', unsafe_allow_html=True)
        else:
            for event in events:
                time_until = event["time"] - datetime.now(timezone.utc)
                hours_away = time_until.total_seconds() / 3600
                danger     = hours_away < 0.5   # within 30 mins = blackout zone

                st.markdown(
                    f'<div class="news-card {"news-high" if danger else "news-medium"}">'
                    f'<div style="font-weight:700; color:{"#ef4444" if danger else "#f97316"};">{"🚫 " if danger else "⚠️ "}{event["name"]}</div>'
                    f'<div style="font-size:0.7rem; color:#94a3b8;">{event["time"].strftime("%H:%M UTC")}</div>'
                    f'<div style="font-size:0.7rem; color:{"#ef4444" if danger else "#64748b"};">In {hours_away:.1f}h</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

        st.markdown("---")
        st.markdown('<div class="section-header">Performance</div>', unsafe_allow_html=True)

        journal = load_journal()
        stats   = calc_stats(journal)

        wr_color  = "#22c55e" if stats["win_rate"] >= 50 else "#ef4444"
        pf_color  = "#22c55e" if stats["profit_factor"] >= 1.5 else "#f97316" if stats["profit_factor"] >= 1 else "#ef4444"
        pnl_color = "#22c55e" if stats["total_pnl"] >= 0 else "#ef4444"

        # Total signals card
        st.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">Total Signals</div>'
            f'<div style="font-size:1.5rem; font-weight:700; color:#f6c90e;">{stats["total_signals"]}</div>'
            f'<div class="metric-label">✅ {stats["auto_executed"]} auto | ⚠️ {stats["manual_alerts"]} manual</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        st.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">Win Rate (closed)</div>'
            f'<div style="font-size:1.5rem; font-weight:700; color:{wr_color};">{stats["win_rate"]}%</div>'
            f'<div class="metric-label">{stats["winners"]}W / {stats["losers"]}L of {stats["total"]} closed</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        st.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">Profit Factor</div>'
            f'<div style="font-size:1.5rem; font-weight:700; color:{pf_color};">{stats["profit_factor"]}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

        st.markdown(
            f'<div class="metric-card">'
            f'<div class="metric-label">Total P&L (closed)</div>'
            f'<div style="font-size:1.5rem; font-weight:700; color:{pnl_color};">{stats["total_pnl"]:+.2f}</div>'
            f'</div>',
            unsafe_allow_html=True
        )

    st.markdown("---")

    # ── TRADE JOURNAL TABLE ──
    st.markdown('<div class="section-header">Trade Journal</div>', unsafe_allow_html=True)

    journal = load_journal()
    if journal.empty:
        st.markdown('<div style="color:#64748b; font-size:0.85rem;">No trades logged yet — journal will populate once trades fire</div>', unsafe_allow_html=True)
    else:
        # Parse timestamps properly
        if "timestamp" in journal.columns:
            journal["timestamp"] = pd.to_datetime(journal["timestamp"], errors="coerce")

        # ── Filter tabs: Today / This Week / All Time ──
        tab_today, tab_week, tab_all = st.tabs(["Today", "This Week", "All Time"])

        # Key columns to display
        display_cols = [
            "timestamp", "symbol", "timeframe", "tier", "confidence_score",
            "entry", "sl", "tp", "session", "status",
            "partial_taken", "be_moved", "exit_reason",
            "actual_pnl", "rr_achieved"
        ]

        def color_pnl(val):
            """Apply green/red colour to P&L values."""
            try:
                v = float(val)
                if v > 0:   return 'color: #22c55e; font-weight: bold'
                elif v < 0: return 'color: #ef4444; font-weight: bold'
            except:
                pass
            return ''

        def render_journal_table(df: pd.DataFrame, label: str) -> None:
            """Render a filtered journal DataFrame as a styled table."""
            if df.empty:
                st.markdown(f'<div style="color:#64748b; font-size:0.85rem; padding:0.5rem 0;">No trades {label}</div>', unsafe_allow_html=True)
                return

            # Only show available columns
            available = [c for c in display_cols if c in df.columns]
            display_df = df[available].copy()

            # Quick summary stats above table
            total    = len(display_df)
            auto_ex  = len(display_df[display_df["status"] == "auto-executed"]) if "status" in display_df.columns else 0
            manual   = len(display_df[display_df["status"] == "manual-alert"]) if "status" in display_df.columns else 0
            ignored  = len(display_df[display_df["status"] == "ignored"]) if "status" in display_df.columns else 0

            s1, s2, s3, s4 = st.columns(4)
            s1.metric("Total Signals", total)
            s2.metric("Auto Executed", auto_ex)
            s3.metric("Manual Alerts", manual)
            s4.metric("Ignored", ignored)

            # Style and render table
            try:
                styled = display_df.style.applymap(
                    color_pnl,
                    subset=["actual_pnl"] if "actual_pnl" in display_df.columns else []
                )
                st.dataframe(styled, use_container_width=True, hide_index=True)
            except Exception:
                st.dataframe(display_df, use_container_width=True, hide_index=True)

        with tab_today:
            today_utc    = datetime.now(timezone.utc).date()
            today_trades = journal[journal["timestamp"].dt.date == today_utc] if "timestamp" in journal.columns else journal
            render_journal_table(today_trades, "today yet")

        with tab_week:
            week_start   = datetime.now(timezone.utc).date() - timedelta(days=7)
            week_trades  = journal[journal["timestamp"].dt.date >= week_start] if "timestamp" in journal.columns else journal
            render_journal_table(week_trades, "this week yet")

        with tab_all:
            render_journal_table(journal, "logged yet")

    # ── FOOTER ──
    st.markdown(
        f'<div style="text-align:center; color:#1e2d4a; font-size:0.65rem; margin-top:1rem; padding-top:1rem; border-top:1px solid #1e2d4a;">'
        f'Manny\'s Gold Bot V4 — Dashboard auto-refreshes every 60s — Last updated: {now.strftime("%H:%M:%S")} UTC'
        f'</div>',
        unsafe_allow_html=True
    )


# ═══════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════

# Connect to MT5
connected = connect_mt5()

if not connected:
    st.error("❌ Failed to connect to MT5. Make sure MetaTrader 5 is running and your .env credentials are correct.")
    st.stop()

# Render the dashboard
render_dashboard()

# Auto-refresh every 60 seconds
# st.rerun() with time.sleep creates a smooth refresh loop
time.sleep(60)
st.rerun()

#streamlit run dashboard.py