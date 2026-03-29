import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import time
import requests
from datetime import datetime, timezone, timedelta

load_dotenv()

# --- CREDENTIALS ---
MT5_LOGIN      = int(os.getenv("MT5_LOGIN"))
MT5_PASSWORD   = os.getenv("MT5_PASSWORD")
MT5_SERVER     = os.getenv("MT5_SERVER")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- SETTINGS ---
SYMBOL         = "XAUUSD"
TIMEFRAME_30   = mt5.TIMEFRAME_M30
TIMEFRAME_4H   = mt5.TIMEFRAME_H4
TIMEFRAME_1H   = mt5.TIMEFRAME_H1
RISK_PERCENT   = 0.01
RR             = 3.0
CHECK_INTERVAL = 60
ADX_PERIOD     = 14
ADX_THRESH     = 25
ATR_PERIOD     = 14
COOLDOWN_BARS  = 8

# --- SESSIONS UTC ---
LONDON_START  = 7
LONDON_END    = 12
OVERLAP_START = 12
OVERLAP_END   = 13
NY_START      = 13
NY_END        = 17
POSTNY_START  = 17
POSTNY_END    = 22

# ═══════════════════════════════════════════
# AUTO DST DETECTION
# ═══════════════════════════════════════════
def get_bias_hours():
    now  = datetime.now(timezone.utc)
    year = now.year

    # Last Sunday of March — BST starts
    march_end = datetime(year, 4, 1, tzinfo=timezone.utc)
    bst_start = march_end - timedelta(days=(march_end.weekday() + 1) % 7)
    bst_start = bst_start.replace(hour=1)

    # Last Sunday of October — BST ends
    oct_end   = datetime(year, 11, 1, tzinfo=timezone.utc)
    bst_end   = oct_end - timedelta(days=(oct_end.weekday() + 1) % 7)
    bst_end   = bst_end.replace(hour=1)

    is_bst = bst_start <= now < bst_end
    return (2, 6) if is_bst else (1, 5)

# ═══════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════
def send_telegram(message):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

# ═══════════════════════════════════════════
# SESSION
# ═══════════════════════════════════════════
def get_session():
    h = datetime.now(timezone.utc).hour
    if   LONDON_START  <= h < LONDON_END:   return "London"
    elif OVERLAP_START <= h < OVERLAP_END:  return "Overlap"
    elif NY_START      <= h < NY_END:       return "New York"
    elif POSTNY_START  <= h < POSTNY_END:   return "Post-NY"
    else:                                   return "Asian"

def is_active_session():
    return get_session() != "Asian"

# ═══════════════════════════════════════════
# MT5 CONNECTION
# ═══════════════════════════════════════════
def connect_mt5():
    if not mt5.initialize(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
        print("MT5 connection failed:", mt5.last_error())
        return False
    return True

# ═══════════════════════════════════════════
# GET CANDLES
# ═══════════════════════════════════════════
def get_candles(timeframe, count=500):
    rates = mt5.copy_rates_from_pos(SYMBOL, timeframe, 0, count)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    return df

# ═══════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series, period=14):
    delta    = series.diff()
    gain     = delta.where(delta > 0, 0)
    loss     = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(span=period).mean()
    avg_loss = loss.ewm(span=period).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    hl = df['high'] - df['low']
    hc = abs(df['high'] - df['close'].shift())
    lc = abs(df['low']  - df['close'].shift())
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(span=period).mean()

def calc_adx(df, period=14):
    plus_dm  = df['high'].diff()
    minus_dm = df['low'].diff().abs()
    plus_dm[plus_dm   < 0] = 0
    minus_dm[minus_dm < 0] = 0
    atr_val  = calc_atr(df, period)
    plus_di  = 100 * (plus_dm.ewm(span=period).mean()  / atr_val)
    minus_di = 100 * (minus_dm.ewm(span=period).mean() / atr_val)
    dx       = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.ewm(span=period).mean()

def calc_macd(series):
    macd   = series.ewm(span=12).mean() - series.ewm(span=26).mean()
    signal = macd.ewm(span=9).mean()
    return macd, signal

# ═══════════════════════════════════════════
# WEEKLY BIAS — Monday 1AM + 5AM 4H candles
# ═══════════════════════════════════════════
def get_weekly_bias(df_4h):
    bias_h1, bias_h2 = get_bias_hours()
    df = df_4h.copy()
    df['dow']  = df['time'].dt.dayofweek  # 0 = Monday
    df['hour'] = df['time'].dt.hour

    # Get most recent Monday bias candles
    mon = df[(df['dow'] == 0) & (df['hour'].isin([bias_h1, bias_h2]))]
    if len(mon) == 0:
        return None, None, False, False, False

    # Most recent ISO week
    latest_week = mon['time'].dt.isocalendar().week.iloc[-1]
    latest_year = mon['time'].dt.isocalendar().year.iloc[-1]
    this_week   = mon[
        (mon['time'].dt.isocalendar().week == latest_week) &
        (mon['time'].dt.isocalendar().year == latest_year)
    ]
    if len(this_week) == 0:
        return None, None, False, False, False

    wb_high = this_week['high'].max()
    wb_low  = this_week['low'].min()

    # Use most recent 4H close for bias confirmation
    c4h = df['close'].iloc[-1]

    wk_bull = c4h > wb_high
    wk_bear = c4h < wb_low

    # Reversal warning — price returned inside range after confirming
    wk_rev = False
    confirmed_bull_bars = df[df['close'] > wb_high]
    confirmed_bear_bars = df[df['close'] < wb_low]
    if len(confirmed_bull_bars) > 0 and (wb_low < c4h < wb_high):
        wk_rev = True
        wk_bull = False
    elif len(confirmed_bear_bars) > 0 and (wb_low < c4h < wb_high):
        wk_rev = True
        wk_bear = False

    return wb_high, wb_low, wk_bull, wk_bear, wk_rev

# ═══════════════════════════════════════════
# DAILY BIAS — 1AM 4H candle each day
# ═══════════════════════════════════════════
def get_daily_bias(df_4h):
    bias_h1, _ = get_bias_hours()
    df   = df_4h.copy()
    df['hour'] = df['time'].dt.hour
    df['date'] = df['time'].dt.date

    # Try today first, then yesterday
    dates = sorted(df['date'].unique())
    daily_candle = None
    for d in reversed(dates):
        match = df[(df['date'] == d) & (df['hour'] == bias_h1)]
        if len(match) > 0:
            daily_candle = match.iloc[-1]
            break

    if daily_candle is None:
        return None, None, False, False

    db_high = daily_candle['high']
    db_low  = daily_candle['low']
    c4h     = df['close'].iloc[-1]

    d_bull = c4h > db_high
    d_bear = c4h < db_low

    return db_high, db_low, d_bull, d_bear

# ═══════════════════════════════════════════
# HTF EMA — 1H or 4H for 30M timeframe
# ═══════════════════════════════════════════
def get_htf_ema(df_1h, df_4h):
    ema200_1h = calc_ema(df_1h['close'], 200).iloc[-1]
    ema200_4h = calc_ema(df_4h['close'], 200).iloc[-1]
    c1h = df_1h['close'].iloc[-1]
    c4h = df_4h['close'].iloc[-1]
    htf_bull = (c1h > ema200_1h) or (c4h > ema200_4h)
    htf_bear = (c1h < ema200_1h) or (c4h < ema200_4h)
    return htf_bull, htf_bear

# ═══════════════════════════════════════════
# PREVIOUS DAY HIGH / LOW
# Uses actual daily boundaries not rolling window
# ═══════════════════════════════════════════
def get_prev_day_hl(df_30):
    df = df_30.copy()
    df['date'] = df['time'].dt.date
    dates = sorted(df['date'].unique())
    if len(dates) < 2:
        return None, None
    prev_day = dates[-2]
    prev = df[df['date'] == prev_day]
    return prev['high'].max(), prev['low'].min()

# ═══════════════════════════════════════════
# PREVIOUS WEEK HIGH / LOW
# ═══════════════════════════════════════════
def get_prev_week_hl(df_30):
    df = df_30.copy()
    df['week'] = df['time'].dt.isocalendar().week
    df['year'] = df['time'].dt.isocalendar().year
    weeks = df[['year','week']].drop_duplicates().values.tolist()
    if len(weeks) < 2:
        return None, None
    py, pw = weeks[-2]
    prev = df[(df['year'] == py) & (df['week'] == pw)]
    return prev['high'].max(), prev['low'].min()

# ═══════════════════════════════════════════
# PREVIOUS MONTH HIGH / LOW
# ═══════════════════════════════════════════
def get_prev_month_hl(df_30):
    df = df_30.copy()
    df['month'] = df['time'].dt.month
    df['year']  = df['time'].dt.year
    months = df[['year','month']].drop_duplicates().values.tolist()
    if len(months) < 2:
        return None, None
    py, pm = months[-2]
    prev = df[(df['year'] == py) & (df['month'] == pm)]
    return prev['high'].max(), prev['low'].min()

# ═══════════════════════════════════════════
# BOS — Break of Structure
# ═══════════════════════════════════════════
def check_bos(df, atr_series, bos_reset=30):
    close  = df['close']
    open_  = df['open']
    ema200 = calc_ema(close, 200)

    sw_high = df['high'].rolling(11, center=True).max()
    sw_low  = df['low'].rolling(11,  center=True).min()

    strong_body = abs(close - open_) > atr_series * 0.5
    above_e200  = close > ema200
    below_e200  = close < ema200

    bull_bos = (close > sw_high.shift(1)) & strong_body & above_e200 & (close.shift(1) <= sw_high.shift(1))
    bear_bos = (close < sw_low.shift(1))  & strong_body & below_e200 & (close.shift(1) >= sw_low.shift(1))

    # Recent BOS within bos_reset bars
    recent_bull = bull_bos.iloc[-bos_reset:].any()
    recent_bear = bear_bos.iloc[-bos_reset:].any()

    return recent_bull, recent_bear

# ═══════════════════════════════════════════
# SWEEPS
# Uses actual previous day high/low
# ═══════════════════════════════════════════
def check_sweeps(df, atr_series, d1h, d1l):
    if d1h is None or d1l is None:
        return False, False, False, False, None, None

    last = df.iloc[-2]
    atr  = atr_series.iloc[-2]

    bull_sweep = (last['low'] < d1l) and (last['close'] > d1l) and ((d1l - last['low']) >= atr * 0.3)
    bear_sweep = (last['high'] > d1h) and (last['close'] < d1h) and ((last['high'] - d1h) >= atr * 0.3)

    bull_sw_rej = bull_sweep and ((last['close'] - last['low'])  > atr * 0.7)
    bear_sw_rej = bear_sweep and ((last['high']  - last['close']) > atr * 0.7)

    last_bull_sw_low  = last['low']  if bull_sweep else None
    last_bear_sw_high = last['high'] if bear_sweep else None

    return bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej, last_bull_sw_low, last_bear_sw_high

# ═══════════════════════════════════════════
# INTERNAL SWEEP
# ═══════════════════════════════════════════
def check_internal_sweep(df, above_e200, below_e200):
    low3  = df['low'].iloc[-4:-1].min()
    high3 = df['high'].iloc[-4:-1].max()
    last  = df.iloc[-2]

    int_bull = (last['low'] < low3)   and (last['close'] > low3)  and above_e200
    int_bear = (last['high'] > high3) and (last['close'] < high3) and below_e200
    return int_bull, int_bear

# ═══════════════════════════════════════════
# EQUAL HIGHS / LOWS SWEPT
# ═══════════════════════════════════════════
def check_eql_swept(df, atr):
    pivot_h = df['high'].rolling(11, center=True).max()
    pivot_l = df['low'].rolling(11,  center=True).min()

    ph  = df['high'].iloc[-12]
    pph = df['high'].iloc[-23]
    pl  = df['low'].iloc[-12]
    ppl = df['low'].iloc[-23]

    last = df.iloc[-2]

    is_eqh = abs(ph - pph) <= atr * 0.1 if not pd.isna(ph) and not pd.isna(pph) else False
    is_eql = abs(pl - ppl) <= atr * 0.1 if not pd.isna(pl) and not pd.isna(ppl) else False

    eqh_swept = is_eqh and (last['high'] > ph) and (last['close'] < ph)
    eql_swept = is_eql and (last['low']  < pl) and (last['close'] > pl)

    return eqh_swept, eql_swept

# ═══════════════════════════════════════════
# FVG
# ═══════════════════════════════════════════
def check_fvg(df, atr_series, above_e200, below_e200,
              wk_bull, wk_bear, day_bull, day_bear,
              bias_med_bull, bias_med_bear,
              bias_sca_bull, bias_sca_bear):
    if len(df) < 5:
        return False, False

    last  = df.iloc[-2]
    prev1 = df.iloc[-3]
    prev2 = df.iloc[-4]
    atr   = atr_series.iloc[-3]

    body    = abs(prev1['close'] - prev1['open'])
    str_cdl = body > atr * 1.0
    disp_cdl = abs(last['close'] - last['open']) > atr_series.iloc[-2] * 1.5

    b_fvg_sz  = prev1['low']  - prev2['high']
    br_fvg_sz = prev2['low']  - prev1['high']

    fvg_min = 0.5
    b_fvg  = (b_fvg_sz  > 0) and str_cdl and (prev1['close'] > prev1['open']) and above_e200 and (b_fvg_sz  >= atr * fvg_min)
    br_fvg = (br_fvg_sz > 0) and str_cdl and (prev1['close'] < prev1['open']) and below_e200 and (br_fvg_sz >= atr * fvg_min)

    bull_fvg_ez = b_fvg  and disp_cdl and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull)
    bear_fvg_ez = br_fvg and disp_cdl and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear)

    return bull_fvg_ez, bear_fvg_ez

# ═══════════════════════════════════════════
# ORDER BLOCKS
# ═══════════════════════════════════════════
def check_ob(df, atr_series, above_e200, below_e200):
    if len(df) < 6:
        return False, False

    close = df['close']
    open_ = df['open']
    imp   = 3

    bull_imp = all(close.iloc[-i] > close.iloc[-i-1] for i in range(1, imp+1))
    bear_imp = all(close.iloc[-i] < close.iloc[-i-1] for i in range(1, imp+1))

    atr = atr_series.iloc[-1]

    bull_ob_cond = bull_imp and (close.iloc[-imp-1] < open_.iloc[-imp-1]) and (abs(close.iloc[-imp-1] - open_.iloc[-imp-1]) > atr * 0.5) and above_e200
    bear_ob_cond = bear_imp and (close.iloc[-imp-1] > open_.iloc[-imp-1]) and (abs(close.iloc[-imp-1] - open_.iloc[-imp-1]) > atr * 0.5) and below_e200

    bull_ob_rsp = False
    bear_ob_rsp = False

    if bull_ob_cond:
        ob_top = df['high'].iloc[-imp-1]
        ob_bot = df['low'].iloc[-imp-1]
        if df['low'].iloc[-1] <= ob_top and df['close'].iloc[-1] >= ob_bot:
            bull_ob_rsp = True

    if bear_ob_cond:
        ob_top = df['high'].iloc[-imp-1]
        ob_bot = df['low'].iloc[-imp-1]
        if df['high'].iloc[-1] >= ob_bot and df['close'].iloc[-1] <= ob_top:
            bear_ob_rsp = True

    return bull_ob_rsp, bear_ob_rsp

# ═══════════════════════════════════════════
# RSI DIVERGENCE
# ═══════════════════════════════════════════
def check_rsi_div(df, rsi_series):
    lkb = 7
    low  = df['low']
    high = df['high']

    p_ll = low.iloc[-lkb:].min()   < low.iloc[-2*lkb:-lkb].min()
    p_hl = low.iloc[-lkb:].min()   > low.iloc[-2*lkb:-lkb].min()
    p_hh = high.iloc[-lkb:].max()  > high.iloc[-2*lkb:-lkb].max()
    p_lh = high.iloc[-lkb:].max()  < high.iloc[-2*lkb:-lkb].max()

    r_hl = rsi_series.iloc[-lkb:].min() > rsi_series.iloc[-2*lkb:-lkb].min()
    r_ll = rsi_series.iloc[-lkb:].min() < rsi_series.iloc[-2*lkb:-lkb].min()
    r_lh = rsi_series.iloc[-lkb:].max() < rsi_series.iloc[-2*lkb:-lkb].max()
    r_hh = rsi_series.iloc[-lkb:].max() > rsi_series.iloc[-2*lkb:-lkb].max()

    return (p_ll and r_hl), (p_hl and r_ll), (p_hh and r_lh), (p_lh and r_hh)

# ═══════════════════════════════════════════
# PLACE TRADE
# ═══════════════════════════════════════════
def place_trade(signal, sl_price, tp_price, lot_size):
    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        send_telegram("❌ Failed to get XAUUSD price!")
        return

    price      = tick.ask if signal == "BUY" else tick.bid
    order_type = mt5.ORDER_TYPE_BUY if signal == "BUY" else mt5.ORDER_TYPE_SELL

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       SYMBOL,
        "volume":       lot_size,
        "type":         order_type,
        "price":        price,
        "sl":           sl_price,
        "tp":           tp_price,
        "deviation":    20,
        "magic":        234000,
        "comment":      "Mannys Gold Bot V3",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result  = mt5.order_send(req)
    account = mt5.account_info()

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        send_telegram(f"❌ <b>TRADE FAILED</b>\nSignal: {signal}\nError: {result.comment}")
        print(f"Trade failed: {result.comment}")
    else:
        send_telegram(
            f"🥇 <b>TRADE PLACED — Manny's Gold V3</b>\n\n"
            f"📊 Signal: {signal}\n"
            f"💰 XAUUSD | {get_session()} Session\n"
            f"📈 Entry: {result.price}\n"
            f"🛑 SL: {sl_price}\n"
            f"🎯 TP: {tp_price}\n"
            f"📦 Lots: {lot_size}\n"
            f"💼 Balance: {account.balance} GBP\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
        )
        print(f"✅ {signal} placed | Entry:{result.price} SL:{sl_price} TP:{tp_price}")

# ═══════════════════════════════════════════
# MAIN SIGNAL CHECK
# ═══════════════════════════════════════════
def check_signal(last_signal_bar, last_bull_bar, last_bear_bar):
    df_30 = get_candles(TIMEFRAME_30, 500)
    df_4h = get_candles(TIMEFRAME_4H, 500)
    df_1h = get_candles(TIMEFRAME_1H, 500)

    if df_30 is None or df_4h is None or df_1h is None:
        print("Failed to get candle data")
        return last_signal_bar, last_bull_bar, last_bear_bar

    if len(df_30) < 210:
        print("Not enough data for EMA200")
        return last_signal_bar, last_bull_bar, last_bear_bar

    # --- INDICATORS ---
    df_30['ema200'] = calc_ema(df_30['close'], 200)
    df_30['ema50']  = calc_ema(df_30['close'], 50)
    df_30['rsi']    = calc_rsi(df_30['close'], 14)
    df_30['atr']    = calc_atr(df_30, ATR_PERIOD)
    df_30['adx']    = calc_adx(df_30, ADX_PERIOD)
    df_30['macd'], df_30['macd_sig'] = calc_macd(df_30['close'])

    # Last CLOSED candle
    last        = df_30.iloc[-2]
    current_bar = last['time']
    bar_index   = len(df_30) - 2

    # Cooldown — 8 bars per direction
    bars_since_bull = (bar_index - last_bull_bar) if last_bull_bar is not None else 999
    bars_since_bear = (bar_index - last_bear_bar) if last_bear_bar is not None else 999
    bull_cooled = bars_since_bull > COOLDOWN_BARS
    bear_cooled = bars_since_bear > COOLDOWN_BARS

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
    pct_below  = (ema200 - close) / ema200 * 100
    ext_bear   = pct_below > 20
    trending   = adx_val >= ADX_THRESH
    macd_bull  = macd_l > macd_s
    macd_bear  = macd_l < macd_s
    rsi_bull_x = rsi < 40
    rsi_bear_x = rsi > 60

    # --- BIAS ---
    wb_high, wb_low, wk_bull, wk_bear, wk_rev = get_weekly_bias(df_4h)
    price_in_weekly_range = (wb_high is not None) and (wb_low is not None) and (wb_low < close < wb_high)
    db_high, db_low, day_bull, day_bear        = get_daily_bias(df_4h)
    htf_bull, htf_bear                         = get_htf_ema(df_1h, df_4h)

    # --- MIXED BIAS ---
    bias_med_bull = wk_bear and day_bull
    bias_med_bear = wk_bull and day_bear
    bias_sca_bull = price_in_weekly_range and day_bull
    bias_sca_bear = price_in_weekly_range and day_bear
    macro_ok      = wk_bull or wk_bear

    # --- GATES ---
    g_str_bull = wk_bull and day_bull and htf_bull and (not ext_bear) and (wk_bull and day_bull)
    g_str_bear = wk_bear and day_bear and htf_bear and (not ext_bear) and (wk_bear and day_bear)
    g_med_bull = wk_bear and day_bull and htf_bull and (not ext_bear)
    g_med_bear = wk_bull and day_bear and htf_bear and (not ext_bear)
    g_sca_bull = price_in_weekly_range and day_bull and htf_bull and (not ext_bear)
    g_sca_bear = price_in_weekly_range and day_bear and htf_bear and (not ext_bear)

    # --- LEVELS ---
    d1h, d1l = get_prev_day_hl(df_30)
    w_high, w_low = get_prev_week_hl(df_30)
    m_high, m_low = get_prev_month_hl(df_30)

    w_mid   = (w_low + (w_high - w_low) / 2) if w_high and w_low else None
    in_disc = (close < w_mid) if w_mid else False
    in_prem = (close > w_mid) if w_mid else False
    m_bull  = (close > m_low  and above_e200) if m_low  else False
    m_bear  = (close < m_high and below_e200) if m_high else False

    # --- BOS ---
    rec_bull_bos, rec_bear_bos = check_bos(df_30, df_30['atr'])

    # --- SWEEPS ---
    bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej, last_bull_sw_low, last_bear_sw_high = check_sweeps(df_30, df_30['atr'], d1h, d1l)

    # --- INTERNAL SWEEP ---
    int_bull, int_bear = check_internal_sweep(df_30, above_e200, below_e200)

    # --- EQUAL HIGHS/LOWS ---
    eqh_swept, eql_swept = check_eql_swept(df_30, atr)

    # --- TIER 2 ---
    s2_long  = sum([above_e200, rec_bull_bos, bull_sweep, in_disc, m_bull])
    s2_short = sum([below_e200, rec_bear_bos, bear_sweep, in_prem, m_bear])

    # --- FVG ---
    bull_fvg_ez, bear_fvg_ez = check_fvg(
        df_30, df_30['atr'],
        above_e200, below_e200,
        wk_bull, wk_bear, day_bull, day_bear,
        bias_med_bull, bias_med_bear,
        bias_sca_bull, bias_sca_bear
    )

    # --- ORDER BLOCKS ---
    bull_ob_rsp, bear_ob_rsp = check_ob(df_30, df_30['atr'], above_e200, below_e200)

    # --- RSI DIVERGENCE ---
    s_bull_div, h_bull_div, s_bear_div, h_bear_div = check_rsi_div(df_30, df_30['rsi'])

    # --- TIER 3 TRIGGERS ---
    bull_wick = last['low']  - min(last['open'], last['close'])
    bear_wick = max(last['open'], last['close']) - last['high']
    pin_body  = abs(last['close'] - last['open'])

    t1_bull = bull_fvg_ez
    t1_bear = bear_fvg_ez
    t2_bull = bull_ob_rsp
    t2_bear = bear_ob_rsp
    t3_bull = bull_sw_rej
    t3_bear = bear_sw_rej
    t4_bull = (bull_wick >= 2.5*pin_body) and (pin_body >= atr*0.1) and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull
    t4_bear = (bear_wick >= 2.5*pin_body) and (pin_body >= atr*0.1) and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear
    t5_bull = (d1h is not None) and (close > d1h) and (df_30['close'].iloc[-3] <= d1h) and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull
    t5_bear = (d1l is not None) and (close < d1l) and (df_30['close'].iloc[-3] >= d1l) and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear

    lh1 = (df_30['high'].iloc[-2] < df_30['high'].iloc[-3]) and (df_30['high'].iloc[-3] < df_30['high'].iloc[-4]) and (df_30['high'].iloc[-4] < df_30['high'].iloc[-5])
    hl1 = (df_30['low'].iloc[-2]  > df_30['low'].iloc[-3])  and (df_30['low'].iloc[-3]  > df_30['low'].iloc[-4])  and (df_30['low'].iloc[-4]  > df_30['low'].iloc[-5])
    t7_bull = lh1 and (last['high'] > df_30['high'].iloc[-3]) and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull
    t7_bear = hl1 and (last['low']  < df_30['low'].iloc[-3])  and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear

    any_trig_bull = t1_bull or t2_bull or t3_bull or t4_bull or t5_bull or t7_bull
    any_trig_bear = t1_bear or t2_bear or t3_bear or t4_bear or t5_bear or t7_bear

    # --- TIER 4 BONUS ---
    bon_l  = 0
    bon_l += 2 if s_bull_div else (1 if h_bull_div else 0)
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

    min_bonus     = 2
    min_bonus_med = max(min_bonus - 1, 0)
    min_bonus_sca = max(min_bonus - 1, 0)
    min_str2      = 2

    # --- SIGNAL FIRING ---
    raw_bull_str = g_str_bull and bull_cooled and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= min_bonus)
    raw_bear_str = g_str_bear and bear_cooled and (s2_short >= min_str2) and any_trig_bear and (bon_s >= min_bonus)
    raw_bull_med = g_med_bull and bull_cooled and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= min_bonus_med)
    raw_bear_med = g_med_bear and bear_cooled and (s2_short >= min_str2) and any_trig_bear and (bon_s >= min_bonus_med)
    raw_bull_sca = g_sca_bull and bull_cooled and (s2_long  >= max(min_str2-1,1)) and any_trig_bull and (bon_l >= min_bonus_sca)
    raw_bear_sca = g_sca_bear and bear_cooled and (s2_short >= max(min_str2-1,1)) and any_trig_bear and (bon_s >= min_bonus_sca)

    raw_bull = raw_bull_str or (raw_bull_med and not raw_bull_str) or (raw_bull_sca and not raw_bull_str and not raw_bull_med)
    raw_bear = raw_bear_str or (raw_bear_med and not raw_bear_str) or (raw_bear_sca and not raw_bear_str and not raw_bear_med)

    bull_tier = "STRONG" if raw_bull_str else ("MEDIUM" if raw_bull_med else ("SCALP" if raw_bull_sca else ""))
    bear_tier = "STRONG" if raw_bear_str else ("MEDIUM" if raw_bear_med else ("SCALP" if raw_bear_sca else ""))

    # --- SL / TP ---
    sl_buf     = atr * 0.5
    long_sl    = round((last_bull_sw_low - sl_buf) if last_bull_sw_low else (close - atr * 1.5), 2)
    long_risk  = max(close - long_sl, 0.0001)
    short_sl   = round((last_bear_sw_high + sl_buf) if last_bear_sw_high else (close + atr * 1.5), 2)
    short_risk = max(short_sl - close, 0.0001)
    long_tp    = round(close + long_risk  * 3, 2)
    short_tp   = round(close - short_risk * 3, 2)

    # --- POSITION SIZING ---
    account  = mt5.account_info()
    balance  = account.balance
    risk_amt = balance * RISK_PERCENT
    long_sz  = max(0.01, round(risk_amt / (long_risk  * 100), 2))
    short_sz = max(0.01, round(risk_amt / (short_risk * 100), 2))

    # --- DST INFO ---
    h1, h2 = get_bias_hours()
    dst_str = "BST (Summer)" if h1 == 2 else "GMT (Winter)"

    # --- STATUS PRINT ---
    print(f"\n{'='*55}")
    print(f"⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC | {get_session()} | {dst_str}")
    print(f"📊 Close:{close:.2f} EMA200:{ema200:.2f} EMA50:{ema50:.2f}")
    print(f"📈 RSI:{rsi:.1f} ADX:{adx_val:.1f} ATR:{atr:.2f}")
    print(f"🧭 WkBull:{wk_bull} WkBear:{wk_bear} | DayBull:{day_bull} DayBear:{day_bear}")
    print(f"📦 WkRange:{wb_high:.2f}-{wb_low:.2f}" if wb_high else "📦 WkRange: Not set yet")
    print(f"🟢 DayRange:{db_high:.2f}-{db_low:.2f}" if db_high else "🟢 DayRange: Not set yet")
    print(f"🔒 HTFBull:{htf_bull} HTFBear:{htf_bear} ExtBear:{ext_bear}")
    print(f"🏗 S2L:{s2_long}/5 S2S:{s2_short}/5 | BonL:{bon_l}/7 BonS:{bon_s}/7")
    print(f"🎯 TrigBull:{any_trig_bull} TrigBear:{any_trig_bear}")
    print(f"⏳ Cooldown — Bull:{bars_since_bull}bars Bear:{bars_since_bear}bars")
    print(f"🚦 Bull:{raw_bull}({bull_tier}) Bear:{raw_bear}({bear_tier})")

    # --- FIRE SIGNAL ---
    if raw_bull:
        emoji = "🔥" if bull_tier == "STRONG" else "💪" if bull_tier == "MEDIUM" else "⚡"
        print(f"\n{emoji} BUY SIGNAL — {bull_tier}")
        send_telegram(
            f"{emoji} <b>{bull_tier} BUY — Manny's Gold V3</b>\n\n"
            f"📊 XAUUSD | {get_session()} | {dst_str}\n"
            f"📈 Entry: {close:.2f}\n"
            f"🛑 SL: {long_sl}\n"
            f"🎯 TP: {long_tp}\n"
            f"📦 Lots: {long_sz}\n"
            f"🏗 S2:{s2_long}/5 Bonus:{bon_l}/7\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
        )
        place_trade("BUY", long_sl, long_tp, long_sz)
        return current_bar, bar_index, last_bear_bar

    elif raw_bear:
        emoji = "🔥" if bear_tier == "STRONG" else "💪" if bear_tier == "MEDIUM" else "⚡"
        print(f"\n{emoji} SELL SIGNAL — {bear_tier}")
        send_telegram(
            f"{emoji} <b>{bear_tier} SELL — Manny's Gold V3</b>\n\n"
            f"📊 XAUUSD | {get_session()} | {dst_str}\n"
            f"📈 Entry: {close:.2f}\n"
            f"🛑 SL: {short_sl}\n"
            f"🎯 TP: {short_tp}\n"
            f"📦 Lots: {short_sz}\n"
            f"🏗 S2:{s2_short}/5 Bonus:{bon_s}/7\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
        )
        place_trade("SELL", short_sl, short_tp, short_sz)
        return current_bar, last_bull_bar, bar_index

    return current_bar, last_bull_bar, last_bear_bar

# ═══════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════
def run():
    print("🚀 Manny's Gold Strategy V3 — Mixed Bias Edition")
    print("📊 STRONG / MEDIUM / SCALP tiers")
    print("🕐 Auto DST detection active")
    print("="*55)

    h1, h2 = get_bias_hours()
    dst = "BST (Summer)" if h1 == 2 else "GMT (Winter)"
    print(f"🕐 Current mode: {dst} | Bias hours: {h1}AM + {h2}AM UTC")

    send_telegram(
        f"🚀 <b>Manny's Gold V3 Started!</b>\n"
        f"📊 Mixed Bias — STRONG/MEDIUM/SCALP\n"
        f"🕐 {dst} | Bias candles: {h1}AM + {h2}AM UTC\n"
        f"⏰ Scanning every 60 seconds\n"
        f"🏦 London | Overlap | NY | Post-NY"
    )

    if not connect_mt5():
        print("Failed to connect to MT5")
        return

    print("✅ MT5 Connected!")

    last_signal_bar = None
    last_bull_bar   = None
    last_bear_bar   = None

    while True:
        now = datetime.now(timezone.utc)
        if is_active_session():
            print(f"\n[{now.strftime('%H:%M:%S')} UTC] Scanning | {get_session()}")
            try:
                last_signal_bar, last_bull_bar, last_bear_bar = check_signal(
                    last_signal_bar, last_bull_bar, last_bear_bar
                )
            except Exception as e:
                print(f"❌ Error: {e}")
                send_telegram(f"⚠️ Scanner error: {e}")
        else:
            h1, _ = get_bias_hours()
            print(f"[{now.strftime('%H:%M:%S')} UTC] 💤 Asian — Next: London 07:00 UTC | Bias: {h1}AM candles")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run()