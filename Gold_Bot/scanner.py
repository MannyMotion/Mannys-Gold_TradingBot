import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import time
import requests
from datetime import datetime, timezone

load_dotenv()

# --- CREDENTIALS ---
MT5_LOGIN = int(os.getenv("MT5_LOGIN"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- SETTINGS ---
SYMBOL       = "XAUUSD"
TIMEFRAME_30 = mt5.TIMEFRAME_M30
TIMEFRAME_4H = mt5.TIMEFRAME_H4
TIMEFRAME_1H = mt5.TIMEFRAME_H1
RISK_PERCENT = 0.01
RR           = 3.0
CHECK_INTERVAL = 60
ADX_PERIOD   = 14
ADX_THRESH   = 25
ATR_PERIOD   = 14

# --- SESSIONS UTC ---
LONDON_START  = 7
LONDON_END    = 12
OVERLAP_START = 12
OVERLAP_END   = 13
NY_START      = 13
NY_END        = 17
POSTNY_START  = 17
POSTNY_END    = 22

# --- TELEGRAM ---
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

# --- SESSION CHECK ---
def get_session():
    now = datetime.now(timezone.utc)
    h = now.hour
    if h >= LONDON_START and h < LONDON_END:
        return "London"
    elif h >= OVERLAP_START and h < OVERLAP_END:
        return "Overlap"
    elif h >= NY_START and h < NY_END:
        return "New York"
    elif h >= POSTNY_START and h < POSTNY_END:
        return "Post-NY"
    else:
        return "Asian"

def is_active_session():
    return get_session() != "Asian"

# --- CONNECT MT5 ---
def connect_mt5():
    if not mt5.initialize(
        login=MT5_LOGIN,
        password=MT5_PASSWORD,
        server=MT5_SERVER
    ):
        print("MT5 connection failed:", mt5.last_error())
        return False
    return True

# --- GET CANDLES ---
def get_candles(timeframe, count=300):
    rates = mt5.copy_rates_from_pos(SYMBOL, timeframe, 0, count)
    if rates is None or len(rates) == 0:
        return None
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s', utc=True)
    return df

# --- INDICATORS ---
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.ewm(span=period).mean()
    avg_loss = loss.ewm(span=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    hl  = df['high'] - df['low']
    hc  = abs(df['high'] - df['close'].shift())
    lc  = abs(df['low']  - df['close'].shift())
    tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.ewm(span=period).mean()

def calc_adx(df, period=14):
    high  = df['high']
    low   = df['low']
    close = df['close']
    plus_dm  = high.diff()
    minus_dm = low.diff().abs()
    plus_dm[plus_dm  < 0] = 0
    minus_dm[minus_dm < 0] = 0
    tr   = calc_atr(df, period)
    plus_di  = 100 * (plus_dm.ewm(span=period).mean()  / tr)
    minus_di = 100 * (minus_dm.ewm(span=period).mean() / tr)
    dx  = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    adx = dx.ewm(span=period).mean()
    return adx, plus_di, minus_di

def calc_macd(series):
    exp1 = series.ewm(span=12, adjust=False).mean()
    exp2 = series.ewm(span=26, adjust=False).mean()
    macd = exp1 - exp2
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

# --- WEEKLY BIAS ---
def get_weekly_bias(df_4h):
    """
    Monday 1AM and 5AM 4H candles form the weekly range.
    If 4H closes above range high -> bull bias
    If 4H closes below range low  -> bear bias
    """
    df = df_4h.copy()
    df['dow'] = df['time'].dt.dayofweek  # 0=Mon
    df['hour'] = df['time'].dt.hour

    # Get most recent Monday 1AM and 5AM candles
    mon_candles = df[(df['dow'] == 0) & (df['hour'].isin([1, 5]))]

    if len(mon_candles) < 1:
        return None, None, False, False, False

    # Get the most recent week's Monday candles
    latest_week = mon_candles['time'].dt.isocalendar().week.iloc[-1]
    latest_year = mon_candles['time'].dt.isocalendar().year.iloc[-1]
    this_week = mon_candles[
        (mon_candles['time'].dt.isocalendar().week == latest_week) &
        (mon_candles['time'].dt.isocalendar().year == latest_year)
    ]

    if len(this_week) == 0:
        return None, None, False, False, False

    wb_high = this_week['high'].max()
    wb_low  = this_week['low'].min()

    # Current 4H close
    current_close = df['close'].iloc[-1]

    # Bias confirmation
    wk_bull = current_close > wb_high
    wk_bear = current_close < wb_low
    wk_rev  = False  # simplified for now

    return wb_high, wb_low, wk_bull, wk_bear, wk_rev

# --- DAILY BIAS ---
def get_daily_bias(df_4h):
    """
    Daily 1AM 4H candle forms the daily range.
    If 4H closes above range high -> bull bias
    If 4H closes below range low  -> bear bias
    """
    df = df_4h.copy()
    df['hour'] = df['time'].dt.hour
    df['date'] = df['time'].dt.date

    today = df['date'].iloc[-1]
    today_1am = df[(df['date'] == today) & (df['hour'] == 1)]

    if len(today_1am) == 0:
        # Try yesterday
        dates = sorted(df['date'].unique())
        if len(dates) >= 2:
            prev_day = dates[-2]
            today_1am = df[(df['date'] == prev_day) & (df['hour'] == 1)]

    if len(today_1am) == 0:
        return None, None, False, False

    db_high = today_1am['high'].iloc[-1]
    db_low  = today_1am['low'].iloc[-1]

    current_close = df['close'].iloc[-1]

    d_bull = current_close > db_high
    d_bear = current_close < db_low

    return db_high, db_low, d_bull, d_bear

# --- HTF EMA CHECK ---
def get_htf_ema(df_1h, df_4h):
    ema200_1h = calc_ema(df_1h['close'], 200).iloc[-1]
    ema200_4h = calc_ema(df_4h['close'], 200).iloc[-1]

    close_1h = df_1h['close'].iloc[-1]
    close_4h = df_4h['close'].iloc[-1]

    bull_1h = close_1h > ema200_1h
    bull_4h = close_4h > ema200_4h
    bear_1h = close_1h < ema200_1h
    bear_4h = close_4h < ema200_4h

    # For 30M: need 1H or 4H
    htf_bull = bull_1h or bull_4h
    htf_bear = bear_1h or bear_4h

    return htf_bull, htf_bear

# --- PIVOT HIGHS AND LOWS ---
def pivot_high(series, left=5, right=5):
    result = pd.Series(np.nan, index=series.index)
    for i in range(left, len(series) - right):
        window = series.iloc[i-left:i+right+1]
        if series.iloc[i] == window.max():
            result.iloc[i] = series.iloc[i]
    return result

def pivot_low(series, left=5, right=5):
    result = pd.Series(np.nan, index=series.index)
    for i in range(left, len(series) - right):
        window = series.iloc[i-left:i+right+1]
        if series.iloc[i] == window.min():
            result.iloc[i] = series.iloc[i]
    return result

# --- BOS ---
def check_bos(df, atr):
    close = df['close']
    high  = df['high']
    low   = df['low']
    open_ = df['open']
    ema200 = calc_ema(close, 200)

    sw_high = df['high'].rolling(11).max().shift(5)
    sw_low  = df['low'].rolling(11).min().shift(5)

    strong_body = abs(close - open_) > atr * 0.5
    above_e200  = close > ema200
    below_e200  = close < ema200

    bull_bos = (close > sw_high) & strong_body & above_e200 & (close.shift(1) <= sw_high)
    bear_bos = (close < sw_low)  & strong_body & below_e200 & (close.shift(1) >= sw_low)

    # Check recent BOS within last 30 bars
    recent_bull_bos = bull_bos.iloc[-30:].any()
    recent_bear_bos = bear_bos.iloc[-30:].any()

    return recent_bull_bos, recent_bear_bos

# --- SWEEPS ---
def check_sweeps(df, atr):
    close = df['close']
    high  = df['high']
    low   = df['low']

    # Previous day high/low approximation
    d1h = high.rolling(48).max().iloc[-1]
    d1l = low.rolling(48).min().iloc[-1]

    last = df.iloc[-2]

    bull_sweep = (last['low'] < d1l) and (last['close'] > d1l) and ((d1l - last['low']) >= atr.iloc[-2] * 0.3)
    bear_sweep = (last['high'] > d1h) and (last['close'] < d1h) and ((last['high'] - d1h) >= atr.iloc[-2] * 0.3)

    bull_sw_rej = bull_sweep and ((last['close'] - last['low']) > atr.iloc[-2] * 0.7)
    bear_sw_rej = bear_sweep and ((last['high'] - last['close']) > atr.iloc[-2] * 0.7)

    last_bull_sw_low  = last['low']  if bull_sweep else None
    last_bear_sw_high = last['high'] if bear_sweep else None

    return bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej, last_bull_sw_low, last_bear_sw_high

# --- FVG ---
def check_fvg(df, atr, above_e200, below_e200, week_bull, week_bear, day_bull, day_bear, bias_med_bull, bias_med_bear, bias_sca_bull, bias_sca_bear):
    last  = df.iloc[-2]
    prev1 = df.iloc[-3]
    prev2 = df.iloc[-4]

    body     = abs(prev1['close'] - prev1['open'])
    str_cdl  = body > atr.iloc[-3] * 1.0
    disp_cdl = abs(last['close'] - last['open']) > atr.iloc[-2] * 1.5

    b_fvg_sz  = prev1['low'] - prev2['high']
    br_fvg_sz = prev2['low'] - prev1['high']

    fvg_min = 0.5
    b_fvg  = (prev1['low'] > prev2['high']) and str_cdl and (prev1['close'] > prev1['open']) and above_e200 and (b_fvg_sz >= atr.iloc[-3] * fvg_min)
    br_fvg = (prev1['high'] < prev2['low']) and str_cdl and (prev1['close'] < prev1['open']) and below_e200 and (br_fvg_sz >= atr.iloc[-3] * fvg_min)

    # FVG entry zone check
    bull_fvg_ez = False
    bear_fvg_ez = False

    if b_fvg and disp_cdl and above_e200 and (week_bull or bias_med_bull or bias_sca_bull):
        bull_fvg_ez = True
    if br_fvg and disp_cdl and below_e200 and (week_bear or bias_med_bear or bias_sca_bear):
        bear_fvg_ez = True

    return bull_fvg_ez, bear_fvg_ez

# --- ORDER BLOCKS ---
def check_ob(df, atr, above_e200, below_e200):
    imp = 3
    close = df['close']
    open_ = df['open']

    bull_imp = (close.iloc[-1] > close.iloc[-2]) and (close.iloc[-2] > close.iloc[-3]) and (close.iloc[-3] > close.iloc[-4])
    bear_imp = (close.iloc[-1] < close.iloc[-2]) and (close.iloc[-2] < close.iloc[-3]) and (close.iloc[-3] < close.iloc[-4])

    bull_ob = bull_imp and (close.iloc[-imp-1] < open_.iloc[-imp-1]) and (abs(close.iloc[-imp-1] - open_.iloc[-imp-1]) > atr.iloc[-1] * 0.5) and above_e200
    bear_ob = bear_imp and (close.iloc[-imp-1] > open_.iloc[-imp-1]) and (abs(close.iloc[-imp-1] - open_.iloc[-imp-1]) > atr.iloc[-1] * 0.5) and below_e200

    if bull_ob:
        ob_top = df['high'].iloc[-imp-1]
        ob_bot = df['low'].iloc[-imp-1]
        # Check if price has returned to OB
        if df['low'].iloc[-1] <= ob_top and df['close'].iloc[-1] >= ob_bot:
            return True, False
    if bear_ob:
        ob_top = df['high'].iloc[-imp-1]
        ob_bot = df['low'].iloc[-imp-1]
        if df['high'].iloc[-1] >= ob_bot and df['close'].iloc[-1] <= ob_top:
            return False, True

    return False, False

# --- RSI DIVERGENCE ---
def check_rsi_div(df, rsi_series):
    lkb = 7
    low  = df['low']
    high = df['high']

    p_ll = low.iloc[-lkb:].min()  < low.iloc[-2*lkb:-lkb].min()
    p_hl = low.iloc[-lkb:].min()  > low.iloc[-2*lkb:-lkb].min()
    p_hh = high.iloc[-lkb:].max() > high.iloc[-2*lkb:-lkb].max()
    p_lh = high.iloc[-lkb:].max() < high.iloc[-2*lkb:-lkb].max()

    r_hl = rsi_series.iloc[-lkb:].min()  > rsi_series.iloc[-2*lkb:-lkb].min()
    r_ll = rsi_series.iloc[-lkb:].min()  < rsi_series.iloc[-2*lkb:-lkb].min()
    r_lh = rsi_series.iloc[-lkb:].max()  < rsi_series.iloc[-2*lkb:-lkb].max()
    r_hh = rsi_series.iloc[-lkb:].max()  > rsi_series.iloc[-2*lkb:-lkb].max()

    s_bull_div = p_ll and r_hl
    h_bull_div = p_hl and r_ll
    s_bear_div = p_hh and r_lh
    h_bear_div = p_lh and r_hh

    return s_bull_div, h_bull_div, s_bear_div, h_bear_div

# --- TIER 2 STRUCTURE SCORE ---
def calc_tier2(above_e200, below_e200, rec_bull_bos, rec_bear_bos, bull_sweep, bear_sweep, in_disc, in_prem, m_bull, m_bear):
    s2_long  = sum([above_e200, rec_bull_bos, bull_sweep, in_disc,  m_bull])
    s2_short = sum([below_e200, rec_bear_bos, bear_sweep, in_prem,  m_bear])
    return s2_long, s2_short

# --- PLACE TRADE ---
def place_trade(signal, sl_price, tp_price, lot_size):
    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        send_telegram("❌ Failed to get XAUUSD price!")
        return

    if signal == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid

    request = {
        "action":      mt5.TRADE_ACTION_DEAL,
        "symbol":      SYMBOL,
        "volume":      lot_size,
        "type":        order_type,
        "price":       price,
        "sl":          sl_price,
        "tp":          tp_price,
        "deviation":   20,
        "magic":       234000,
        "comment":     "Mannys Gold Bot V3",
        "type_time":   mt5.ORDER_TIME_GTC,
        "type_filling":mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    account = mt5.account_info()

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        msg = (
            f"❌ <b>TRADE FAILED</b>\n"
            f"Signal: {signal}\n"
            f"Error: {result.comment}"
        )
        send_telegram(msg)
        print(f"Trade failed: {result.comment}")
    else:
        msg = (
            f"🥇 <b>TRADE PLACED — Manny's Gold V3</b>\n\n"
            f"📊 Signal: {signal}\n"
            f"💰 Symbol: {SYMBOL}\n"
            f"📈 Entry: {result.price}\n"
            f"🛑 Stop Loss: {sl_price}\n"
            f"🎯 Take Profit: {tp_price}\n"
            f"📦 Lot Size: {lot_size}\n"
            f"💼 Balance: {account.balance} GBP\n"
            f"⏰ Time: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC\n"
            f"🏦 Session: {get_session()}"
        )
        send_telegram(msg)
        print(f"✅ {signal} placed at {result.price} | SL:{sl_price} TP:{tp_price}")

# --- MAIN SIGNAL CHECK ---
def check_signal(last_signal_bar):
    # Get candle data
    df_30  = get_candles(TIMEFRAME_30, 300)
    df_4h  = get_candles(TIMEFRAME_4H, 300)
    df_1h  = get_candles(TIMEFRAME_1H, 300)

    if df_30 is None or df_4h is None or df_1h is None:
        print("Failed to get candle data")
        return last_signal_bar

    if len(df_30) < 50:
        print("Not enough data")
        return last_signal_bar

    # --- INDICATORS ---
    df_30['ema200'] = calc_ema(df_30['close'], 200)
    df_30['ema50']  = calc_ema(df_30['close'], 50)
    df_30['rsi']    = calc_rsi(df_30['close'], 14)
    df_30['atr']    = calc_atr(df_30, ATR_PERIOD)
    df_30['adx'], df_30['plus_di'], df_30['minus_di'] = calc_adx(df_30, ADX_PERIOD)
    df_30['macd'], df_30['macd_sig'] = calc_macd(df_30['close'])

    # Use last CLOSED candle
    last    = df_30.iloc[-2]
    current_bar = last['time']

    # Cooldown check
    if last_signal_bar is not None and current_bar == last_signal_bar:
        print(f"⏳ Cooldown active")
        return last_signal_bar

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
    bull_mkt   = ema50 > ema200
    pct_below  = (ema200 - close) / ema200 * 100
    ext_bear   = pct_below > 20
    trending   = adx_val >= ADX_THRESH
    macd_bull  = macd_l > macd_s
    macd_bear  = macd_l < macd_s
    rsi_bull_x = rsi < 40
    rsi_bear_x = rsi > 60

    # --- WEEKLY BIAS ---
    wb_high, wb_low, wk_bull, wk_bear, wk_rev = get_weekly_bias(df_4h)
    price_in_weekly_range = (wb_high is not None) and (wb_low is not None) and (close < wb_high) and (close > wb_low)

    # --- DAILY BIAS ---
    db_high, db_low, day_bull, day_bear = get_daily_bias(df_4h)

    # --- HTF EMA ---
    htf_bull, htf_bear = get_htf_ema(df_1h, df_4h)

    # --- MIXED BIAS TIERS ---
    bias_strong_bull = wk_bull and day_bull
    bias_strong_bear = wk_bear and day_bear
    bias_med_bull    = wk_bear and day_bull
    bias_med_bear    = wk_bull and day_bear
    bias_sca_bull    = price_in_weekly_range and day_bull
    bias_sca_bear    = price_in_weekly_range and day_bear
    macro_ok         = wk_bull or wk_bear
    macro_trend_bull = wk_bull and day_bull
    macro_trend_bear = wk_bear and day_bear

    # --- GATE CONDITIONS ---
    # STRONG gates
    g_str_bull = wk_bull and day_bull and htf_bull and (not ext_bear) and macro_trend_bull
    g_str_bear = wk_bear and day_bear and htf_bear and (not ext_bear) and macro_trend_bear

    # MEDIUM gates
    g_med_bull = wk_bear and day_bull and htf_bull and (not ext_bear)
    g_med_bear = wk_bull and day_bear and htf_bear and (not ext_bear)

    # SCALP gates
    g_sca_bull = price_in_weekly_range and day_bull and htf_bull and (not ext_bear)
    g_sca_bear = price_in_weekly_range and day_bear and htf_bear and (not ext_bear)

    # Session gate
    active_sess = is_active_session()
    if not active_sess:
        print(f"💤 Outside session")
        return last_signal_bar

    # --- WEEKLY LEVELS ---
    w_high = df_30['high'].rolling(336).max().iloc[-1]  # ~1 week of 30M bars
    w_low  = df_30['low'].rolling(336).min().iloc[-1]
    w_mid  = w_low + (w_high - w_low) / 2
    in_disc = close < w_mid
    in_prem = close > w_mid

    # Monthly alignment
    m_high = df_30['high'].rolling(1440).max().iloc[-1]
    m_low  = df_30['low'].rolling(1440).min().iloc[-1]
    m_bull = close > m_low and above_e200
    m_bear = close < m_high and below_e200

    # --- BOS ---
    rec_bull_bos, rec_bear_bos = check_bos(df_30, df_30['atr'])

    # --- SWEEPS ---
    bull_sweep, bear_sweep, bull_sw_rej, bear_sw_rej, last_bull_sw_low, last_bear_sw_high = check_sweeps(df_30, df_30['atr'])

    # --- TIER 2 ---
    s2_long, s2_short = calc_tier2(
        above_e200, below_e200,
        rec_bull_bos, rec_bear_bos,
        bull_sweep, bear_sweep,
        in_disc, in_prem,
        m_bull, m_bear
    )

    # --- FVG ---
    bull_fvg_ez, bear_fvg_ez = check_fvg(
        df_30, df_30['atr'],
        above_e200, below_e200,
        wk_bull, wk_bear,
        day_bull, day_bear,
        bias_med_bull, bias_med_bear,
        bias_sca_bull, bias_sca_bear
    )

    # --- ORDER BLOCKS ---
    bull_ob_rsp, bear_ob_rsp = check_ob(df_30, df_30['atr'], above_e200, below_e200)

    # --- RSI DIVERGENCE ---
    s_bull_div, h_bull_div, s_bear_div, h_bear_div = check_rsi_div(df_30, df_30['rsi'])
    any_bull_div = s_bull_div or h_bull_div
    any_bear_div = s_bear_div or h_bear_div

    # --- TIER 3 TRIGGERS ---
    t1_bull = bull_fvg_ez
    t1_bear = bear_fvg_ez
    t2_bull = bull_ob_rsp
    t2_bear = bear_ob_rsp
    t3_bull = bull_sw_rej
    t3_bear = bear_sw_rej

    # Pin bar trigger
    bull_wick = last['low']  - min(last['open'], last['close'])
    bear_wick = max(last['open'], last['close']) - last['high']
    pin_body  = abs(last['close'] - last['open'])
    t4_bull = (bull_wick >= 2.5 * pin_body) and (pin_body >= atr * 0.1) and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull
    t4_bear = (bear_wick >= 2.5 * pin_body) and (pin_body >= atr * 0.1) and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear

    # PDH/PDL break
    d1h = df_30['high'].rolling(48).max().iloc[-2]
    d1l = df_30['low'].rolling(48).min().iloc[-2]
    prev_close = df_30['close'].iloc[-3]
    t5_bull = (close > d1h) and (prev_close <= d1h) and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull
    t5_bear = (close < d1l) and (prev_close >= d1l) and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear

    # CHoCH
    lh1 = (df_30['high'].iloc[-2] < df_30['high'].iloc[-3]) and (df_30['high'].iloc[-3] < df_30['high'].iloc[-4]) and (df_30['high'].iloc[-4] < df_30['high'].iloc[-5])
    hl1 = (df_30['low'].iloc[-2]  > df_30['low'].iloc[-3])  and (df_30['low'].iloc[-3]  > df_30['low'].iloc[-4])  and (df_30['low'].iloc[-4]  > df_30['low'].iloc[-5])
    t7_bull = lh1 and (last['high'] > df_30['high'].iloc[-3]) and above_e200 and (wk_bull or bias_med_bull or bias_sca_bull) and day_bull
    t7_bear = hl1 and (last['low']  < df_30['low'].iloc[-3])  and below_e200 and (wk_bear or bias_med_bear or bias_sca_bear) and day_bear

    any_trig_bull = t1_bull or t2_bull or t3_bull or t4_bull or t5_bull or t7_bull
    any_trig_bear = t1_bear or t2_bear or t3_bear or t4_bear or t5_bear or t7_bear

    # --- TIER 4 BONUS ---
    bon_l = 0
    bon_l += 2 if s_bull_div else (1 if h_bull_div else 0)
    bon_l += 1 if macd_bull  else 0
    bon_l += 1 if rsi_bull_x else 0
    bon_l += 1 if trending   else 0

    bon_s = 0
    bon_s += 2 if s_bear_div else (1 if h_bear_div else 0)
    bon_s += 1 if macd_bear  else 0
    bon_s += 1 if rsi_bear_x else 0
    bon_s += 1 if trending   else 0

    min_bonus     = 2
    min_bonus_med = max(min_bonus - 1, 0)
    min_bonus_sca = max(min_bonus - 1, 0)
    min_str2      = 2

    # --- SIGNAL FIRING ---
    raw_bull_str = g_str_bull and active_sess and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= min_bonus)
    raw_bear_str = g_str_bear and active_sess and (s2_short >= min_str2) and any_trig_bear and (bon_s >= min_bonus)
    raw_bull_med = g_med_bull and active_sess and (s2_long  >= min_str2) and any_trig_bull and (bon_l >= min_bonus_med)
    raw_bear_med = g_med_bear and active_sess and (s2_short >= min_str2) and any_trig_bear and (bon_s >= min_bonus_med)
    raw_bull_sca = g_sca_bull and active_sess and (s2_long  >= max(min_str2-1, 1)) and any_trig_bull and (bon_l >= min_bonus_sca)
    raw_bear_sca = g_sca_bear and active_sess and (s2_short >= max(min_str2-1, 1)) and any_trig_bear and (bon_s >= min_bonus_sca)

    raw_bull = raw_bull_str or (raw_bull_med and not raw_bull_str) or (raw_bull_sca and not raw_bull_str and not raw_bull_med)
    raw_bear = raw_bear_str or (raw_bear_med and not raw_bear_str) or (raw_bear_sca and not raw_bear_str and not raw_bear_med)

    bull_tier = "STRONG" if raw_bull_str else ("MEDIUM" if raw_bull_med else ("SCALP" if raw_bull_sca else ""))
    bear_tier = "STRONG" if raw_bear_str else ("MEDIUM" if raw_bear_med else ("SCALP" if raw_bear_sca else ""))

    # --- SL / TP CALCULATION ---
    sl_buf   = atr * 0.5
    long_sl  = (last_bull_sw_low - sl_buf) if last_bull_sw_low else (close - atr * 1.5)
    long_sl  = round(long_sl, 2)
    long_risk = max(close - long_sl, 0.0001)

    short_sl  = (last_bear_sw_high + sl_buf) if last_bear_sw_high else (close + atr * 1.5)
    short_sl  = round(short_sl, 2)
    short_risk = max(short_sl - close, 0.0001)

    long_tp  = round(close + long_risk  * 3, 2)
    short_tp = round(close - short_risk * 3, 2)

    # --- POSITION SIZING ---
    account  = mt5.account_info()
    balance  = account.balance
    risk_amt = balance * RISK_PERCENT
    long_sz  = round(risk_amt / (long_risk  * 100), 2)
    short_sz = round(risk_amt / (short_risk * 100), 2)
    long_sz  = max(0.01, long_sz)
    short_sz = max(0.01, short_sz)

    # --- PRINT STATUS ---
    print(f"\n{'='*50}")
    print(f"⏰ {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC | Session: {get_session()}")
    print(f"📊 Close:{close:.2f} EMA200:{ema200:.2f} EMA50:{ema50:.2f}")
    print(f"📈 RSI:{rsi:.1f} ADX:{adx_val:.1f} ATR:{atr:.2f}")
    print(f"🧭 WkBull:{wk_bull} WkBear:{wk_bear} DayBull:{day_bull} DayBear:{day_bear}")
    print(f"🔒 HTFBull:{htf_bull} HTFBear:{htf_bear} ExtBear:{ext_bear}")
    print(f"🏗 S2L:{s2_long} S2S:{s2_short} | BonL:{bon_l} BonS:{bon_s}")
    print(f"🎯 TrigBull:{any_trig_bull} TrigBear:{any_trig_bear}")
    print(f"🚦 RawBull:{raw_bull}({bull_tier}) RawBear:{raw_bear}({bear_tier})")

    # --- FIRE SIGNAL ---
    if raw_bull:
        tier_emoji = "🔥" if bull_tier == "STRONG" else "💪" if bull_tier == "MEDIUM" else "⚡"
        print(f"\n{tier_emoji} BUY SIGNAL FIRED! Tier: {bull_tier}")
        send_telegram(
            f"{tier_emoji} <b>{bull_tier} BUY SIGNAL</b>\n\n"
            f"📊 Manny's Gold Strategy V3\n"
            f"💰 XAUUSD | {get_session()} Session\n"
            f"📈 Entry: {close:.2f}\n"
            f"🛑 SL: {long_sl}\n"
            f"🎯 TP: {long_tp}\n"
            f"📦 Lots: {long_sz}\n"
            f"🏗 S2: {s2_long}/5 | Bonus: {bon_l}/7\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
        )
        place_trade("BUY", long_sl, long_tp, long_sz)
        return current_bar

    elif raw_bear:
        tier_emoji = "🔥" if bear_tier == "STRONG" else "💪" if bear_tier == "MEDIUM" else "⚡"
        print(f"\n{tier_emoji} SELL SIGNAL FIRED! Tier: {bear_tier}")
        send_telegram(
            f"{tier_emoji} <b>{bear_tier} SELL SIGNAL</b>\n\n"
            f"📊 Manny's Gold Strategy V3\n"
            f"💰 XAUUSD | {get_session()} Session\n"
            f"📈 Entry: {close:.2f}\n"
            f"🛑 SL: {short_sl}\n"
            f"🎯 TP: {short_tp}\n"
            f"📦 Lots: {short_sz}\n"
            f"🏗 S2: {s2_short}/5 | Bonus: {bon_s}/7\n"
            f"⏰ {datetime.now(timezone.utc).strftime('%H:%M')} UTC"
        )
        place_trade("SELL", short_sl, short_tp, short_sz)
        return current_bar

    return last_signal_bar

# --- MAIN LOOP ---
def run():
    print("🚀 Manny's Gold Strategy V3 Scanner Starting...")
    print("📊 Mixed Bias Edition — STRONG / MEDIUM / SCALP")
    print("="*50)

    send_telegram(
        "🚀 <b>Manny's Gold Strategy V3 Started!</b>\n"
        "📊 Mixed Bias Edition\n"
        "🎯 STRONG / MEDIUM / SCALP tiers active\n"
        "⏰ Scanning every 60 seconds\n"
        "🏦 London | NY | Post-NY sessions"
    )

    if not connect_mt5():
        print("Failed to connect to MT5")
        return

    print("✅ MT5 Connected!")
    last_signal_bar = None

    while True:
        now = datetime.now(timezone.utc)
        if is_active_session():
            print(f"\n[{now.strftime('%H:%M:%S')} UTC] 🔍 Scanning | Session: {get_session()}")
            try:
                last_signal_bar = check_signal(last_signal_bar)
            except Exception as e:
                print(f"Error: {e}")
                send_telegram(f"⚠️ Scanner error: {e}")
        else:
            print(f"[{now.strftime('%H:%M:%S')} UTC] 💤 Asian session — waiting for London 07:00 UTC")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run()