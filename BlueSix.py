import MetaTrader5 as mt5
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# Constants
SYMBOL = "EURUSD"
TIMEFRAME = mt5.TIMEFRAME_M1
MIN_PIPETTES_BODY = 10
MIN_PIPETTES_WICK = 5
MIN_PIPETTES_DIFF = 3
MIN_PIPETTES_00_59 = 5  # Closing price of 00 to opening price of 59
MIN_PIPETTES_01_59 = 5  # Closing price of 01 to opening price of 59
MIN_PIPETTES_INTERVAL = 5  # Minimum pipettes for intervals (10, 15, 20, 30 minutes, and full hour)

# Initialize MT5 connection
if not mt5.initialize():
    print("Failed to initialize MT5, error code =", mt5.last_error())
    quit()

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)
cursor = db.cursor()

# Create the patterns table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS patterns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    pattern_name VARCHAR(255) NOT NULL,
    three_minute_trend ENUM('Bull', 'Bear') NOT NULL,
    full_hour_bull INT DEFAULT 0,
    full_hour_bear INT DEFAULT 0,
    first_10_min_bull INT DEFAULT 0,
    first_10_min_bear INT DEFAULT 0,
    first_15_min_bull INT DEFAULT 0,
    first_15_min_bear INT DEFAULT 0,
    first_20_min_bull INT DEFAULT 0,
    first_20_min_bear INT DEFAULT 0,
    first_30_min_bull INT DEFAULT 0,
    first_30_min_bear INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
db.commit()

def get_candles(symbol, timeframe, start, end):
    """
    Fetch candlestick data from MT5.
    """
    rates = mt5.copy_rates_range(symbol, timeframe, start, end)
    if rates is None:
        print("Failed to fetch candlestick data.")
        return None
    return pd.DataFrame(rates)

def analyze_candle_set(x, y, z):
    """
    Analyze a set of three candles (x, y, z) and assign a group identifier based on conditions.
    """
    x_open = x['open']
    y_close = y['close']
    z_close = z['close']

    # Check if the minimum pipettes conditions are met
    if abs(y_close - x_open) < MIN_PIPETTES_00_59 or abs(z_close - x_open) < MIN_PIPETTES_01_59:
        return None

    # Define identifiers based on comparisons
    identifier = ""

    # Bull/Bull/Bull
    if x_open < y_close < z_close:
        identifier = "Bull_Bull_Bull_1"
    # Bear/Bear/Bear
    elif x_open > y_close > z_close:
        identifier = "Bear_Bear_Bear_1"
    # Bull/Bear/Bull
    elif x_open < y_close > z_close:
        if x_open < y_close < z_close:
            identifier = "Bull_Bear_Bull_1"
        elif x_open > y_close and x_open > z_close:
            identifier = "Bull_Bear_Bull_2"
        elif x_open > y_close and x_open < z_close and y_close > z_close:
            identifier = "Bull_Bear_Bull_3"
        elif x_open > y_close and x_open < z_close and y_close < z_close:
            identifier = "Bull_Bear_Bull_4"
    # Bear/Bull/Bear
    elif x_open > y_close < z_close:
        if x_open > y_close > z_close:
            identifier = "Bear_Bull_Bear_1"
        elif x_open < y_close and x_open < z_close:
            identifier = "Bear_Bull_Bear_2"
        elif x_open < y_close and x_open > z_close and y_close < z_close:
            identifier = "Bear_Bull_Bear_3"
        elif x_open < y_close and x_open > z_close and y_close > z_close:
            identifier = "Bear_Bull_Bear_4"
    # Bull/Bull/Bear
    elif x_open < y_close < z_close:
        identifier = "Bull_Bull_Bear_1"
    elif x_open < y_close and y_close > z_close and x_open < z_close:
        identifier = "Bull_Bull_Bear_2"
    elif x_open < y_close and x_open > z_close:
        identifier = "Bull_Bull_Bear_3"
    # Bear/Bear/Bull
    elif x_open > y_close > z_close:
        identifier = "Bear_Bear_Bull_1"
    elif x_open > y_close and y_close < z_close and x_open > z_close:
        identifier = "Bear_Bear_Bull_2"
    elif x_open > y_close and x_open < z_close:
        identifier = "Bear_Bear_Bull_3"
    # Bear/Bull/Bull
    elif x_open > y_close and x_open > z_close:
        identifier = "Bear_Bull_Bull_1"
    elif x_open > y_close and x_open < z_close:
        identifier = "Bear_Bull_Bull_2"
    elif x_open < y_close < z_close:
        identifier = "Bear_Bull_Bull_3"
    # Bull/Bear/Bear
    elif x_open < y_close and x_open < z_close:
        identifier = "Bull_Bear_Bear_1"
    elif x_open < y_close and x_open > z_close:
        identifier = "Bull_Bear_Bear_2"
    elif x_open > y_close > z_close:
        identifier = "Bull_Bear_Bear_3"

    return identifier

def fetch_historical_data(symbol, timeframe, hours=2000):
    """
    Fetch historical candlestick data for the past specified hours.
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    rates = mt5.copy_rates_range(symbol, timeframe, start_time, end_time)
    if rates is None:
        print("Failed to fetch historical data.")
        return None
    return pd.DataFrame(rates)

def analyze_historical_data(data):
    """
    Analyze historical data and calculate percentage ratios for each pattern.
    """
    patterns = {}  # To store patterns and their bull/bear counts

    # Iterate through the data in steps of 60 (to get 59, 00, 01 of each hour)
    for i in range(2, len(data), 60):
        # Ensure we have enough data for the next hour
        if i + 60 > len(data):
            break

        # Get the three candles (59, 00, 01)
        x = data.iloc[i - 2]  # 59th minute
        y = data.iloc[i - 1]  # 00th minute
        z = data.iloc[i]      # 01st minute

        # Analyze the three candles
        identifier = analyze_candle_set(x, y, z)
        if identifier:
            # Determine the 3-minute trend (bull or bear)
            three_minute_trend = "Bull" if z['close'] > x['open'] else "Bear"

            # Get the hour of the 00th minute candle (y)
            hour_start = y['time'].replace(minute=0, second=0, microsecond=0)
            hour_end = hour_start + timedelta(hours=1)

            # Fetch all candles for the current hour (00:00 to 59:59)
            hour_candles = data[(data['time'] >= hour_start) & (data['time'] < hour_end)]

            # Initialize counts for the current pattern
            if identifier not in patterns:
                patterns[identifier] = {
                    "full_hour_bull": 0,
                    "full_hour_bear": 0,
                    "first_10_min_bull": 0,
                    "first_10_min_bear": 0,
                    "first_15_min_bull": 0,
                    "first_15_min_bear": 0,
                    "first_20_min_bull": 0,
                    "first_20_min_bear": 0,
                    "first_30_min_bull": 0,
                    "first_30_min_bear": 0,
                }

            # Determine if the hour is bull or bear (full hour)
            if len(hour_candles) > 0:
                hour_open = hour_candles.iloc[0]['open']
                hour_close = hour_candles.iloc[-1]['close']
                hour_pipettes = abs(hour_close - hour_open)

                # Check if the full hour meets the minimum pipettes requirement
                if hour_pipettes >= MIN_PIPETTES_INTERVAL:
                    hour_candle_type = "Bull" if hour_close > hour_open else "Bear"
                    if hour_candle_type == "Bull":
                        patterns[identifier]["full_hour_bull"] += 1
                    else:
                        patterns[identifier]["full_hour_bear"] += 1

            # Check the first 10 minutes (00:00 to 09:59)
            first_10_min_candles = hour_candles[(hour_candles['time'] >= hour_start) & (hour_candles['time'] < hour_start + timedelta(minutes=10))]
            if len(first_10_min_candles) > 0:
                first_10_min_open = first_10_min_candles.iloc[0]['open']
                first_10_min_close = first_10_min_candles.iloc[-1]['close']
                first_10_min_pipettes = abs(first_10_min_close - first_10_min_open)

                # Check if the first 10 minutes meet the minimum pipettes requirement
                if first_10_min_pipettes >= MIN_PIPETTES_INTERVAL:
                    first_10_min_trend = "Bull" if first_10_min_close > first_10_min_open else "Bear"
                    if first_10_min_trend == "Bull":
                        patterns[identifier]["first_10_min_bull"] += 1
                    else:
                        patterns[identifier]["first_10_min_bear"] += 1

            # Check the first 15 minutes (00:00 to 14:59)
            first_15_min_candles = hour_candles[(hour_candles['time'] >= hour_start) & (hour_candles['time'] < hour_start + timedelta(minutes=15))]
            if len(first_15_min_candles) > 0:
                first_15_min_open = first_15_min_candles.iloc[0]['open']
                first_15_min_close = first_15_min_candles.iloc[-1]['close']
                first_15_min_pipettes = abs(first_15_min_close - first_15_min_open)

                # Check if the first 15 minutes meet the minimum pipettes requirement
                if first_15_min_pipettes >= MIN_PIPETTES_INTERVAL:
                    first_15_min_trend = "Bull" if first_15_min_close > first_15_min_open else "Bear"
                    if first_15_min_trend == "Bull":
                        patterns[identifier]["first_15_min_bull"] += 1
                    else:
                        patterns[identifier]["first_15_min_bear"] += 1

            # Check the first 20 minutes (00:00 to 19:59)
            first_20_min_candles = hour_candles[(hour_candles['time'] >= hour_start) & (hour_candles['time'] < hour_start + timedelta(minutes=20))]
            if len(first_20_min_candles) > 0:
                first_20_min_open = first_20_min_candles.iloc[0]['open']
                first_20_min_close = first_20_min_candles.iloc[-1]['close']
                first_20_min_pipettes = abs(first_20_min_close - first_20_min_open)

                # Check if the first 20 minutes meet the minimum pipettes requirement
                if first_20_min_pipettes >= MIN_PIPETTES_INTERVAL:
                    first_20_min_trend = "Bull" if first_20_min_close > first_20_min_open else "Bear"
                    if first_20_min_trend == "Bull":
                        patterns[identifier]["first_20_min_bull"] += 1
                    else:
                        patterns[identifier]["first_20_min_bear"] += 1

            # Check the first 30 minutes (00:00 to 29:59)
            first_30_min_candles = hour_candles[(hour_candles['time'] >= hour_start) & (hour_candles['time'] < hour_start + timedelta(minutes=30))]
            if len(first_30_min_candles) > 0:
                first_30_min_open = first_30_min_candles.iloc[0]['open']
                first_30_min_close = first_30_min_candles.iloc[-1]['close']
                first_30_min_pipettes = abs(first_30_min_close - first_30_min_open)

                # Check if the first 30 minutes meet the minimum pipettes requirement
                if first_30_min_pipettes >= MIN_PIPETTES_INTERVAL:
                    first_30_min_trend = "Bull" if first_30_min_close > first_30_min_open else "Bear"
                    if first_30_min_trend == "Bull":
                        patterns[identifier]["first_30_min_bull"] += 1
                    else:
                        patterns[identifier]["first_30_min_bear"] += 1

    # Calculate percentage ratios for each pattern
    for pattern, counts in patterns.items():
        # Full hour trend
        full_hour_total = counts["full_hour_bull"] + counts["full_hour_bear"]
        full_hour_bull_ratio = (counts["full_hour_bull"] / full_hour_total) * 100 if full_hour_total > 0 else 0
        full_hour_bear_ratio = (counts["full_hour_bear"] / full_hour_total) * 100 if full_hour_total > 0 else 0

        # First 10 minutes trend
        first_10_min_total = counts["first_10_min_bull"] + counts["first_10_min_bear"]
        first_10_min_bull_ratio = (counts["first_10_min_bull"] / first_10_min_total) * 100 if first_10_min_total > 0 else 0
        first_10_min_bear_ratio = (counts["first_10_min_bear"] / first_10_min_total) * 100 if first_10_min_total > 0 else 0

        # First 15 minutes trend
        first_15_min_total = counts["first_15_min_bull"] + counts["first_15_min_bear"]
        first_15_min_bull_ratio = (counts["first_15_min_bull"] / first_15_min_total) * 100 if first_15_min_total > 0 else 0
        first_15_min_bear_ratio = (counts["first_15_min_bear"] / first_15_min_total) * 100 if first_15_min_total > 0 else 0

        # First 20 minutes trend
        first_20_min_total = counts["first_20_min_bull"] + counts["first_20_min_bear"]
        first_20_min_bull_ratio = (counts["first_20_min_bull"] / first_20_min_total) * 100 if first_20_min_total > 0 else 0
        first_20_min_bear_ratio = (counts["first_20_min_bear"] / first_20_min_total) * 100 if first_20_min_total > 0 else 0

        # First 30 minutes trend
        first_30_min_total = counts["first_30_min_bull"] + counts["first_30_min_bear"]
        first_30_min_bull_ratio = (counts["first_30_min_bull"] / first_30_min_total) * 100 if first_30_min_total > 0 else 0
        first_30_min_bear_ratio = (counts["first_30_min_bear"] / first_30_min_total) * 100 if first_30_min_total > 0 else 0

        # Print the results
        print(f"Pattern: {pattern}")
        print(f"Full Hour Trend: Bull={counts['full_hour_bull']}, Bear={counts['full_hour_bear']}, Bull Ratio={full_hour_bull_ratio:.2f}%, Bear Ratio={full_hour_bear_ratio:.2f}%")
        print(f"First 10 Minutes: Bull={counts['first_10_min_bull']}, Bear={counts['first_10_min_bear']}, Bull Ratio={first_10_min_bull_ratio:.2f}%, Bear Ratio={first_10_min_bear_ratio:.2f}%")
        print(f"First 15 Minutes: Bull={counts['first_15_min_bull']}, Bear={counts['first_15_min_bear']}, Bull Ratio={first_15_min_bull_ratio:.2f}%, Bear Ratio={first_15_min_bear_ratio:.2f}%")
        print(f"First 20 Minutes: Bull={counts['first_20_min_bull']}, Bear={counts['first_20_min_bear']}, Bull Ratio={first_20_min_bull_ratio:.2f}%, Bear Ratio={first_20_min_bear_ratio:.2f}%")
        print(f"First 30 Minutes: Bull={counts['first_30_min_bull']}, Bear={counts['first_30_min_bear']}, Bull Ratio={first_30_min_bull_ratio:.2f}%, Bear Ratio={first_30_min_bear_ratio:.2f}%")
        print("-" * 50)

def store_pattern(pattern_name, three_minute_trend, full_hour_bull, full_hour_bear, first_10_min_bull, first_10_min_bear, first_15_min_bull, first_15_min_bear, first_20_min_bull, first_20_min_bear, first_30_min_bull, first_30_min_bear):
    """
    Store the pattern in the MySQL database.
    """
    # Check if the pattern already exists
    cursor.execute("""
        SELECT full_hour_bull, full_hour_bear, first_10_min_bull, first_10_min_bear, first_15_min_bull, first_15_min_bear, first_20_min_bull, first_20_min_bear, first_30_min_bull, first_30_min_bear FROM patterns 
        WHERE pattern_name = %s AND three_minute_trend = %s
    """, (pattern_name, three_minute_trend))
    result = cursor.fetchone()

    if result:
        # Update the counts if the pattern exists
        new_full_hour_bull = result[0] + full_hour_bull
        new_full_hour_bear = result[1] + full_hour_bear
        new_first_10_min_bull = result[2] + first_10_min_bull
        new_first_10_min_bear = result[3] + first_10_min_bear
        new_first_15_min_bull = result[4] + first_15_min_bull
        new_first_15_min_bear = result[5] + first_15_min_bear
        new_first_20_min_bull = result[6] + first_20_min_bull
        new_first_20_min_bear = result[7] + first_20_min_bear
        new_first_30_min_bull = result[8] + first_30_min_bull
        new_first_30_min_bear = result[9] + first_30_min_bear

        cursor.execute("""
            UPDATE patterns 
            SET full_hour_bull = %s, full_hour_bear = %s,
                first_10_min_bull = %s, first_10_min_bear = %s,
                first_15_min_bull = %s, first_15_min_bear = %s,
                first_20_min_bull = %s, first_20_min_bear = %s,
                first_30_min_bull = %s, first_30_min_bear = %s
            WHERE pattern_name = %s AND three_minute_trend = %s
        """, (
            new_full_hour_bull, new_full_hour_bear,
            new_first_10_min_bull, new_first_10_min_bear,
            new_first_15_min_bull, new_first_15_min_bear,
            new_first_20_min_bull, new_first_20_min_bear,
            new_first_30_min_bull, new_first_30_min_bear,
            pattern_name, three_minute_trend
        ))
    else:
        # Insert a new pattern if it doesn't exist
        cursor.execute("""
            INSERT INTO patterns (
                pattern_name, three_minute_trend,
                full_hour_bull, full_hour_bear,
                first_10_min_bull, first_10_min_bear,
                first_15_min_bull, first_15_min_bear,
                first_20_min_bull, first_20_min_bear,
                first_30_min_bull, first_30_min_bear
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            pattern_name, three_minute_trend,
            full_hour_bull, full_hour_bear,
            first_10_min_bull, first_10_min_bear,
            first_15_min_bull, first_15_min_bear,
            first_20_min_bull, first_20_min_bear,
            first_30_min_bull, first_30_min_bear
        ))
    db.commit()

def main():
    # Fetch historical data
    historical_data = fetch_historical_data(SYMBOL, TIMEFRAME, hours=2000)
    if historical_data is not None:
        analyze_historical_data(historical_data)

# Run the main function
if __name__ == "__main__":
    main()

# Close connections
cursor.close()
db.close()
mt5.shutdown()