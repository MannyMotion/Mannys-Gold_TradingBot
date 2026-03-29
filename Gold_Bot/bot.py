from flask import Flask, request, jsonify
import MetaTrader5 as mt5
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

app = Flask(__name__)

# --- CREDENTIALS ---
MT5_LOGIN = int(os.getenv("MT5_LOGIN"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

# --- SETTINGS ---
SYMBOL = "XAUUSD"
RISK_PERCENT = 0.01
RR = 3.0
SL_DISTANCE = 10.0

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

# --- MT5 CONNECTION ---
def connect_mt5():
    if not mt5.initialize(
        login=MT5_LOGIN,
        password=MT5_PASSWORD,
        server=MT5_SERVER
    ):
        print("MT5 connection failed:", mt5.last_error())
        return False
    return True

# --- POSITION SIZING ---
def calculate_position_size(balance, sl_distance):
    risk_amount = balance * RISK_PERCENT
    lot_size = round(risk_amount / (sl_distance * 100), 2)
    return max(0.01, lot_size)

# --- PLACE TRADE ---
def place_trade(signal):
    if not connect_mt5():
        send_telegram("❌ MT5 connection failed!")
        return

    account = mt5.account_info()
    balance = account.balance

    tick = mt5.symbol_info_tick(SYMBOL)
    if tick is None:
        send_telegram("❌ Failed to get XAUUSD price!")
        mt5.shutdown()
        return

    # Determine direction
    if signal == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
        sl = round(price - SL_DISTANCE, 2)
        tp = round(price + (SL_DISTANCE * RR), 2)
    elif signal == "SELL":
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
        sl = round(price + SL_DISTANCE, 2)
        tp = round(price - (SL_DISTANCE * RR), 2)
    else:
        print("Unknown signal:", signal)
        mt5.shutdown()
        return

    lot_size = calculate_position_size(balance, SL_DISTANCE)

    request_data = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": SYMBOL,
        "volume": lot_size,
        "type": order_type,
        "price": price,
        "sl": sl,
        "tp": tp,
        "deviation": 20,
        "magic": 234000,
        "comment": "Mannys Gold Bot",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request_data)

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        msg = (
            f"❌ <b>TRADE FAILED</b>\n"
            f"Signal: {signal}\n"
            f"Error: {result.comment}\n"
            f"Code: {result.retcode}"
        )
        send_telegram(msg)
        print("Trade failed:", result.comment)
    else:
        msg = (
            f"✅ <b>TRADE PLACED!</b>\n\n"
            f"📊 Signal: {signal}\n"
            f"💰 Symbol: {SYMBOL}\n"
            f"📈 Entry: {result.price}\n"
            f"🛑 Stop Loss: {sl}\n"
            f"🎯 Take Profit: {tp}\n"
            f"📦 Lot Size: {lot_size}\n"
            f"💼 Balance: {balance} GBP\n"
            f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}"
        )
        send_telegram(msg)
        print("Trade placed successfully!", result.order)

    mt5.shutdown()

# --- WEBHOOK ENDPOINT ---
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json

    # Security check
    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    signal = data.get("signal", "").upper()
    print(f"Signal received: {signal}")

    if signal in ["BUY", "SELL"]:
        place_trade(signal)
        return jsonify({"status": "Trade executed", "signal": signal}), 200
    else:
        return jsonify({"error": "Invalid signal"}), 400

# --- HEALTH CHECK ---
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "Mannys Gold Bot is running!",
        "symbol": SYMBOL,
        "risk": "1%"
    }), 200

if __name__ == "__main__":
    print("🚀 Mannys Gold Bot Starting...")
    send_telegram(
        "🚀 <b>Mannys Gold Bot Started!</b>\n"
        "✅ Webhook server is live\n"
        "📊 Waiting for TradingView signals..."
    )
    app.run(host="0.0.0.0", port=5000, debug=False)