import MetaTrader5 as mt5
import pandas as pd

# === CONFIGURATION ===
SYMBOL = "EURUSD"  # Change to your broker's symbol if needed
TIMEFRAME = mt5.TIMEFRAME_M1  # 1-minute timeframe
CANDLES = 50000000  # Number of candles to fetch

# === CONNECT TO MT5 ===
if not mt5.initialize():
    print("❌ Failed to connect to MT5")
    exit()

# ✅ Ensure the symbol is selected in Market Watch
if not mt5.symbol_select(SYMBOL, True):
    print(f"❌ Failed to select {SYMBOL}")
    mt5.shutdown()
    exit()

# === FETCH CANDLESTICK DATA ===
rates = mt5.copy_rates_from_pos(SYMBOL, TIMEFRAME, 0, CANDLES)

# === CHECK IF DATA RETRIEVED ===
if rates is None or len(rates) == 0:
    print("⚠️ Failed to retrieve candlestick data")
    mt5.shutdown()
    exit()

# === CONVERT DATA TO DATAFRAME ===
df = pd.DataFrame(rates)
df['time'] = pd.to_datetime(df['time'], unit='s')  # Convert timestamp to readable date

# === DISPLAY DATA ===
print(df[['time', 'open', 'high', 'low', 'close']])

# === SHUTDOWN MT5 CONNECTION ===
mt5.shutdown()
