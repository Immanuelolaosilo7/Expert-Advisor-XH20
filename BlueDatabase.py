import MetaTrader5 as mt5
import mysql.connector
from datetime import datetime
from tabulate import tabulate  # For tabular output

# Function to calculate pipettes based on asset type
def calculate_pipettes(price1, price2, symbol):
    if "JPY" in symbol:  # JPY pairs have 2 decimal places
        return abs(price1 - price2) * 100  # 1 pipette = 0.01
    else:  # Forex pairs, metals, stocks (assuming 4 decimal places)
        return abs(price1 - price2) * 10000  # 1 pipette = 0.0001

# Connect to MetaTrader 5
if not mt5.initialize():
    print("Initialize() failed, error code =", mt5.last_error())
    quit()

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)
cursor = db.cursor()

# Define symbols to fetch data for
symbols = ["EURUSD", "USDJPY", "XAUUSD", "AAPL"]  # Add more symbols as needed

# Define timeframe and number of candles to fetch
timeframe = mt5.TIMEFRAME_M1  # 1-minute timeframe (adjust as needed)
num_candles = 500  # Fetch last 500 candles (adjust as needed)

# Minimum pipettes requirements
min_body_pipettes = 10
min_wick_pipettes = 5
max_second_wick_pipettes = 3

# List to store table data
table_data = []

# Process each symbol
for symbol in symbols:
    print(f"Processing symbol: {symbol}")

    # Fetch candlestick data
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_candles)

    # Process each candle
    for i in range(len(rates) - 1):
        candle = rates[i]
        next_candle = rates[i + 1]

        open_price = candle['open']
        close_price = candle['close']
        high_price = candle['high']
        low_price = candle['low']

        # Calculate pipettes for body and wicks
        body_pipettes = calculate_pipettes(open_price, close_price, symbol)
        upper_wick_pipettes = calculate_pipettes(high_price, max(open_price, close_price), symbol)
        lower_wick_pipettes = calculate_pipettes(min(open_price, close_price), low_price, symbol)

        # Check conditions
        if (body_pipettes >= min_body_pipettes and
            (upper_wick_pipettes >= min_wick_pipettes or lower_wick_pipettes >= min_wick_pipettes) and
            (upper_wick_pipettes <= max_second_wick_pipettes or lower_wick_pipettes <= max_second_wick_pipettes)):

            # Determine candle type (bull or bear)
            candle_type = "Bull" if close_price > open_price else "Bear"

            # Determine wick type
            if upper_wick_pipettes >= min_wick_pipettes:
                wick_type = "Upper"
            elif lower_wick_pipettes >= min_wick_pipettes:
                wick_type = "Lower"
            else:
                wick_type = "None"

            # Calculate pipettes for the longer wick
            longer_wick_pipettes = max(upper_wick_pipettes, lower_wick_pipettes)

            # Calculate difference between body and longer wick
            body_wick_diff = abs(body_pipettes - longer_wick_pipettes)

            # Check successor candle conditions
            next_open_price = next_candle['open']
            next_close_price = next_candle['close']
            next_body_pipettes = calculate_pipettes(next_open_price, next_close_price, symbol)

            if next_body_pipettes >= min_body_pipettes:
                # Store data in MySQL
                sql = """
                INSERT INTO candles (date_time, symbol, candle_type, wick_type, body_pipettes, longer_wick_pipettes, body_wick_diff, next_candle_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    datetime.fromtimestamp(candle['time']), symbol, candle_type, wick_type,
                    body_pipettes, longer_wick_pipettes, body_wick_diff,
                    "Bull" if next_close_price > next_open_price else "Bear"
                )
                cursor.execute(sql, values)
                db.commit()

                # Add data to table
                table_data.append([
                    datetime.fromtimestamp(candle['time']), symbol, candle_type, wick_type,
                    body_pipettes, longer_wick_pipettes, body_wick_diff,
                    "Bull" if next_close_price > next_open_price else "Bear"
                ])

# Display data in a table
headers = [
    "Date/Time", "Symbol", "Candle Type", "Wick Type",
    "Body Pipettes", "Longer Wick Pipettes", "Body & Wick Difference", "Next Candle Type"
]
print(tabulate(table_data, headers=headers, tablefmt="pretty"))

# Close connections
cursor.close()
db.close()
mt5.shutdown()