import MetaTrader5 as mt5
import time
import random
import requests

# Telegram Bot Token and Chat ID
TELEGRAM_BOT_TOKEN = "7862662931:AAGJaLSzXQNoWLDtul6IQqIJPz-lU0UbfR0"  # Replace with your bot token
TELEGRAM_CHAT_ID = "7692014027"  # Replace with your chat ID

# Function to send a message to Telegram
def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # Raise an error for bad status codes
        print("✅ Message sent to Teleg")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send message: {e}")
        if response:
            print(f"Response: {response.text}")  # Print the API response for debugging

# 1️⃣ Connect to MetaTrader 5
if not mt5.initialize():
    print("❌ Failed to connect to MT5")
    quit()

# 2️⃣ Define Trading Symbol and Lot Size
symbol = "EURUSD"  # Change this to your preferred symbol
lot_size = 0.1  # Small lot size for testing

# 3️⃣ Continuous Trading Loop
while True:
    # Fetch latest price data
    symbol_info = mt5.symbol_info_tick(symbol)
    if symbol_info is None:
        print(f"❌ Failed to get price data for {symbol}")
        break

    ask_price = symbol_info.ask  # Buy price
    bid_price = symbol_info.bid  # Sell price

    # Generate a random magic number for tracking orders
    magic_number = random.randint(10000, 99999)

    # 4️⃣ Place a Random BUY Order
    buy_order = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lot_size,
        "type": mt5.ORDER_TYPE_BUY,
        "price": ask_price,
        "deviation": 10,
        "magic": magic_number,
        "comment": "Test Buy Order",
        "type_time": mt5.ORDER_TIME_GTC
    }

    order_result = mt5.order_send(buy_order)

    if order_result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"❌ Order failed: {order_result.comment}")
        break

    # Send trade alert message to Telegram
    trade_alert = (
        "📢 Trade Alert!\n"
        f"🔹 BUY {symbol}\n"
        f"🔹 Volume: {lot_size}\n"
        f"🔹 Price: {ask_price:.5f}\n"
        f"🔹 Ticket: {order_result.order}"
    )
    print(trade_alert)
    send_telegram_message(trade_alert)

    # 5️⃣ Wait a Few Seconds Before Closing
    time.sleep(random.randint(3, 10))  # Random wait time (3-10 seconds)

    # 6️⃣ Close the Order
    open_positions = mt5.positions_get(symbol=symbol)

    if open_positions:
        for position in open_positions:
            # Debug: Print the position object
            print("Debug - Position Object:", position)

            close_order = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": position.volume,
                "type": mt5.ORDER_TYPE_SELL,  # Opposite action to close
                "price": bid_price,
                "position": position.ticket,
                "deviation": 10,
                "magic": position.magic,
                "comment": "Test Close Order",
                "type_time": mt5.ORDER_TIME_GTC
            }

            close_result = mt5.order_send(close_order)

            if close_result.retcode == mt5.TRADE_RETCODE_DONE:
                # Construct the close trade alert message
                close_alert = (
                    "📢 Trade Alert!\n"
                    f"🔹 CLOSE {symbol}\n"
                    f"🔹 Volume: {position.volume}\n"
                    f"🔹 Price: {bid_price:.5f}\n"
                    f"🔹 Ticket: {position.ticket}"
                )
                print("Debug - Close Alert Message:")  # Debug statement
                print(close_alert)  # Debug statement
                send_telegram_message(close_alert)  # Send the formatted message
            else:
                print(f"❌ Failed to close order {position.ticket}: {close_result.comment}")
                send_telegram_message(f"❌ Failed to close order {position.ticket}: {close_result.comment}")

    else:
        print("⚠ No open positions found to close.")
        send_telegram_message("⚠ No open positions found to close.")

    # 7️⃣ Wait a Few Seconds Before Next Trade
    time.sleep(random.randint(5, 15))  # Random wait time (5-15 seconds)

# 8️⃣ Disconnect from MT5 (In case of error)
mt5.shutdown()