import MetaTrader5 as mt5
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from tabulate import tabulate  # For tabular data formatting

# Database connection details
db_config = {
    'user': 'MudangXh20',
    'password': 'Olaos',
    'host': 'localhost',
    'database': '20'
}

# Minimum pipettes/pip values (can be set by the user)
MIN_BODY_PIPETTES = 3  # Example: 3 pipettes
MIN_LONG_WICK_PIPETTES = 3  # Example: 3 pipettes
MIN_DIFF_BODY_LONG_WICK = 2  # Example: 2 pipettes
MIN_DIFF_LONG_SHORT_WICK = 1  # Example: 1 pipette
MIN_DIFF_BODY_SHORT_WICK = 0  # Example: 0 pipettes

# Minimum pipettes for intervals
MIN_PIPETTES_FULL_HOUR = 10  # Example: 10 pipettes for full hour
MIN_PIPETTES_10_MIN = 5  # Example: 5 pipettes for first 10 minutes
MIN_PIPETTES_15_MIN = 7  # Example: 7 pipettes for first 15 minutes
MIN_PIPETTES_20_MIN = 8  # Example: 8 pipettes for first 20 minutes
MIN_PIPETTES_30_MIN = 10  # Example: 10 pipettes for first 30 minutes

# Candlestick pattern identifiers
PATTERN_IDS = {
    'BU1': 'Body > Long Wick (Upper) > Short Wick',
    'BU2': 'Body > Long Wick (Lower) > Short Wick',
    'BU3': 'Body < Long Wick (Upper) > Short Wick',
    'BU4': 'Body < Long Wick (Lower) > Short Wick',
    'BU5': 'Body < Long Wick (Upper), Short Wick > Body',
    'BU6': 'Body < Long Wick (Lower), Short Wick > Body',
    'BD1': 'Bearish Body > Long Wick (Upper) > Short Wick',
    'BD2': 'Bearish Body > Long Wick (Lower) > Short Wick',
    'BD3': 'Bearish Body < Long Wick (Upper) > Short Wick',
    'BD4': 'Bearish Body < Long Wick (Lower) > Short Wick',
    'BD5': 'Bearish Body < Long Wick (Upper), Short Wick > Body',
    'BD6': 'Bearish Body < Long Wick (Lower), Short Wick > Body'
}

def connect_to_db():
    return mysql.connector.connect(**db_config)

def create_table(conn):
    cursor = conn.cursor()
    # Create candlestick_patterns table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS candlestick_patterns (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pattern_name VARCHAR(50),
            five_minute_trend ENUM('bull', 'bear'),
            full_hour_bull_count INT DEFAULT 0,
            full_hour_bear_count INT DEFAULT 0,
            first_10_min_bull_count INT DEFAULT 0,
            first_10_min_bear_count INT DEFAULT 0,
            first_15_min_bull_count INT DEFAULT 0,
            first_15_min_bear_count INT DEFAULT 0,
            first_20_min_bull_count INT DEFAULT 0,
            first_20_min_bear_count INT DEFAULT 0,
            first_30_min_bull_count INT DEFAULT 0,
            first_30_min_bear_count INT DEFAULT 0,
            PRIMARY KEY (pattern_name, five_minute_trend)
        )
    ''')
    # Create candles table for tabulated data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS candles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pattern_name VARCHAR(50),
            five_minute_trend ENUM('bull', 'bear'),
            full_hour_buy INT DEFAULT 0,
            full_hour_sell INT DEFAULT 0,
            first_10_min_buy INT DEFAULT 0,
            first_10_min_sell INT DEFAULT 0,
            first_15_min_buy INT DEFAULT 0,
            first_15_min_sell INT DEFAULT 0,
            first_20_min_buy INT DEFAULT 0,
            first_20_min_sell INT DEFAULT 0,
            first_30_min_buy INT DEFAULT 0,
            first_30_min_sell INT DEFAULT 0
        )
    ''')
    conn.commit()

def analyze_candlestick(candle):
    open_price, close_price, high_price, low_price = candle
    body = abs(close_price - open_price)
    upper_wick = high_price - max(open_price, close_price)
    lower_wick = min(open_price, close_price) - low_price
    long_wick = max(upper_wick, lower_wick)
    short_wick = min(upper_wick, lower_wick)
    is_bullish = close_price > open_price

    # Check conditions
    if (body >= MIN_BODY_PIPETTES and
        long_wick >= MIN_LONG_WICK_PIPETTES and
        abs(body - long_wick) >= MIN_DIFF_BODY_LONG_WICK and
        abs(long_wick - short_wick) >= MIN_DIFF_LONG_SHORT_WICK and
        abs(body - short_wick) >= MIN_DIFF_BODY_SHORT_WICK and
        body % long_wick != 0 and long_wick % body != 0):

        if body > long_wick and long_wick > short_wick:
            if upper_wick > lower_wick:
                return 'BU1' if is_bullish else 'BD1'
            else:
                return 'BU2' if is_bullish else 'BD2'
        elif body < long_wick and long_wick > short_wick:
            if upper_wick > lower_wick:
                return 'BU3' if is_bullish else 'BD3'
            else:
                return 'BU4' if is_bullish else 'BD4'
        elif body < long_wick and short_wick > body:
            if upper_wick > lower_wick:
                return 'BU5' if is_bullish else 'BD5'
            else:
                return 'BU6' if is_bullish else 'BD6'
    return None

def analyze_hourly_data(data):
    patterns = []
    for i in range(0, len(data), 5):
        hour_data = data[i:i+5]
        patterns_in_hour = []
        for candle in hour_data:
            pattern = analyze_candlestick(candle)
            if pattern:
                patterns_in_hour.append(pattern)
        if len(patterns_in_hour) == 5:
            # Determine 5-minute trend: compare open at 58th minute to close at 02nd minute
            five_minute_trend = 'bull' if hour_data[-1][1] > hour_data[0][0] else 'bear'
            
            # Determine full-hour trend: compare open at 00th minute to close at 59th minute
            full_hour_pipettes = abs(data[i+2][1] - data[i+2][0])  # Pipettes for full hour
            if full_hour_pipettes >= MIN_PIPETTES_FULL_HOUR:
                full_hour_trend = 'bull' if data[i+2][1] > data[i+2][0] else 'bear'
            else:
                full_hour_trend = None  # Skip if pipettes requirement is not met
            
            # Determine first 10 minutes trend: compare open at 00th minute to close at 09th minute
            first_10_min_pipettes = abs(data[i+2][1] - data[i+2][0])  # Pipettes for first 10 minutes
            if first_10_min_pipettes >= MIN_PIPETTES_10_MIN:
                first_10_min_trend = 'bull' if data[i+2][1] > data[i+2][0] else 'bear'
            else:
                first_10_min_trend = None  # Skip if pipettes requirement is not met
            
            # Determine first 15 minutes trend: compare open at 00th minute to close at 14th minute
            first_15_min_pipettes = abs(data[i+2][1] - data[i+2][0])  # Pipettes for first 15 minutes
            if first_15_min_pipettes >= MIN_PIPETTES_15_MIN:
                first_15_min_trend = 'bull' if data[i+2][1] > data[i+2][0] else 'bear'
            else:
                first_15_min_trend = None  # Skip if pipettes requirement is not met
            
            # Determine first 20 minutes trend: compare open at 00th minute to close at 19th minute
            first_20_min_pipettes = abs(data[i+2][1] - data[i+2][0])  # Pipettes for first 20 minutes
            if first_20_min_pipettes >= MIN_PIPETTES_20_MIN:
                first_20_min_trend = 'bull' if data[i+2][1] > data[i+2][0] else 'bear'
            else:
                first_20_min_trend = None  # Skip if pipettes requirement is not met
            
            # Determine first 30 minutes trend: compare open at 00th minute to close at 29th minute
            first_30_min_pipettes = abs(data[i+2][1] - data[i+2][0])  # Pipettes for first 30 minutes
            if first_30_min_pipettes >= MIN_PIPETTES_30_MIN:
                first_30_min_trend = 'bull' if data[i+2][1] > data[i+2][0] else 'bear'
            else:
                first_30_min_trend = None  # Skip if pipettes requirement is not met
            
            pattern_name = '-'.join(patterns_in_hour)
            patterns.append((pattern_name, five_minute_trend, full_hour_trend, first_10_min_trend, first_15_min_trend, first_20_min_trend, first_30_min_trend))
    return patterns

def save_to_db(conn, patterns):
    cursor = conn.cursor()
    for pattern_name, five_minute_trend, full_hour_trend, first_10_min_trend, first_15_min_trend, first_20_min_trend, first_30_min_trend in patterns:
        # Update candlestick_patterns table
        if full_hour_trend == 'bull':
            cursor.execute('''
                INSERT INTO candlestick_patterns (pattern_name, five_minute_trend, full_hour_bull_count)
                VALUES (%s, %s, 1)
                ON DUPLICATE KEY UPDATE full_hour_bull_count = full_hour_bull_count + 1
            ''', (pattern_name, five_minute_trend))
        elif full_hour_trend == 'bear':
            cursor.execute('''
                INSERT INTO candlestick_patterns (pattern_name, five_minute_trend, full_hour_bear_count)
                VALUES (%s, %s, 1)
                ON DUPLICATE KEY UPDATE full_hour_bear_count = full_hour_bear_count + 1
            ''', (pattern_name, five_minute_trend))
        
        # Update candles table with tabulated data
        cursor.execute('''
            INSERT INTO candles (
                pattern_name, five_minute_trend,
                full_hour_buy, full_hour_sell,
                first_10_min_buy, first_10_min_sell,
                first_15_min_buy, first_15_min_sell,
                first_20_min_buy, first_20_min_sell,
                first_30_min_buy, first_30_min_sell
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                full_hour_buy = full_hour_buy + VALUES(full_hour_buy),
                full_hour_sell = full_hour_sell + VALUES(full_hour_sell),
                first_10_min_buy = first_10_min_buy + VALUES(first_10_min_buy),
                first_10_min_sell = first_10_min_sell + VALUES(first_10_min_sell),
                first_15_min_buy = first_15_min_buy + VALUES(first_15_min_buy),
                first_15_min_sell = first_15_min_sell + VALUES(first_15_min_sell),
                first_20_min_buy = first_20_min_buy + VALUES(first_20_min_buy),
                first_20_min_sell = first_20_min_sell + VALUES(first_20_min_sell),
                first_30_min_buy = first_30_min_buy + VALUES(first_30_min_buy),
                first_30_min_sell = first_30_min_sell + VALUES(first_30_min_sell)
        ''', (
            pattern_name, five_minute_trend,
            1 if full_hour_trend == 'bull' else 0, 1 if full_hour_trend == 'bear' else 0,
            1 if first_10_min_trend == 'bull' else 0, 1 if first_10_min_trend == 'bear' else 0,
            1 if first_15_min_trend == 'bull' else 0, 1 if first_15_min_trend == 'bear' else 0,
            1 if first_20_min_trend == 'bull' else 0, 1 if first_20_min_trend == 'bear' else 0,
            1 if first_30_min_trend == 'bull' else 0, 1 if first_30_min_trend == 'bear' else 0
        ))
    conn.commit()

def calculate_bull_bear_ratio(conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT pattern_name,
               five_minute_trend,
               full_hour_bull_count,
               full_hour_bear_count,
               first_10_min_bull_count,
               first_10_min_bear_count,
               first_15_min_bull_count,
               first_15_min_bear_count,
               first_20_min_bull_count,
               first_20_min_bear_count,
               first_30_min_bull_count,
               first_30_min_bear_count
        FROM candlestick_patterns
    ''')
    results = cursor.fetchall()
    for pattern_name, five_minute_trend, full_hour_bull_count, full_hour_bear_count, first_10_min_bull_count, first_10_min_bear_count, first_15_min_bull_count, first_15_min_bear_count, first_20_min_bull_count, first_20_min_bear_count, first_30_min_bull_count, first_30_min_bear_count in results:
        # Calculate full-hour ratio
        full_hour_total = full_hour_bull_count + full_hour_bear_count
        if full_hour_total > 0:
            full_hour_bull_ratio = (full_hour_bull_count / full_hour_total) * 100
            full_hour_bear_ratio = (full_hour_bear_count / full_hour_total) * 100
            print(f'Pattern: {pattern_name}, 5-Minute Trend: {five_minute_trend}, Full Hour Buy: {full_hour_bull_count}, Full Hour Sell: {full_hour_bear_count}, Full Hour Buy Ratio: {full_hour_bull_ratio:.2f}%, Full Hour Sell Ratio: {full_hour_bear_ratio:.2f}%')
        
        # Calculate first 10 minutes ratio
        first_10_min_total = first_10_min_bull_count + first_10_min_bear_count
        if first_10_min_total > 0:
            first_10_min_bull_ratio = (first_10_min_bull_count / first_10_min_total) * 100
            first_10_min_bear_ratio = (first_10_min_bear_count / first_10_min_total) * 100
            print(f'Pattern: {pattern_name}, 5-Minute Trend: {five_minute_trend}, First 10 Min Buy: {first_10_min_bull_count}, First 10 Min Sell: {first_10_min_bear_count}, First 10 Min Buy Ratio: {first_10_min_bull_ratio:.2f}%, First 10 Min Sell Ratio: {first_10_min_bear_ratio:.2f}%')
        
        # Calculate first 15 minutes ratio
        first_15_min_total = first_15_min_bull_count + first_15_min_bear_count
        if first_15_min_total > 0:
            first_15_min_bull_ratio = (first_15_min_bull_count / first_15_min_total) * 100
            first_15_min_bear_ratio = (first_15_min_bear_count / first_15_min_total) * 100
            print(f'Pattern: {pattern_name}, 5-Minute Trend: {five_minute_trend}, First 15 Min Buy: {first_15_min_bull_count}, First 15 Min Sell: {first_15_min_bear_count}, First 15 Min Buy Ratio: {first_15_min_bull_ratio:.2f}%, First 15 Min Sell Ratio: {first_15_min_bear_ratio:.2f}%')
        
        # Calculate first 20 minutes ratio
        first_20_min_total = first_20_min_bull_count + first_20_min_bear_count
        if first_20_min_total > 0:
            first_20_min_bull_ratio = (first_20_min_bull_count / first_20_min_total) * 100
            first_20_min_bear_ratio = (first_20_min_bear_count / first_20_min_total) * 100
            print(f'Pattern: {pattern_name}, 5-Minute Trend: {five_minute_trend}, First 20 Min Buy: {first_20_min_bull_count}, First 20 Min Sell: {first_20_min_bear_count}, First 20 Min Buy Ratio: {first_20_min_bull_ratio:.2f}%, First 20 Min Sell Ratio: {first_20_min_bear_ratio:.2f}%')
        
        # Calculate first 30 minutes ratio
        first_30_min_total = first_30_min_bull_count + first_30_min_bear_count
        if first_30_min_total > 0:
            first_30_min_bull_ratio = (first_30_min_bull_count / first_30_min_total) * 100
            first_30_min_bear_ratio = (first_30_min_bear_count / first_30_min_total) * 100
            print(f'Pattern: {pattern_name}, 5-Minute Trend: {five_minute_trend}, First 30 Min Buy: {first_30_min_bull_count}, First 30 Min Sell: {first_30_min_bear_count}, First 30 Min Buy Ratio: {first_30_min_bull_ratio:.2f}%, First 30 Min Sell Ratio: {first_30_min_bear_ratio:.2f}%')

def fetch_data_from_mt5(symbol, timeframe, start_time, end_time):
    # Initialize MT5 connection
    if not mt5.initialize():
        print("Failed to initialize MT5")
        return None

    # Fetch data
    rates = mt5.copy_rates_range(symbol, timeframe, start_time, end_time)
    mt5.shutdown()

    if rates is None:
        print("No data fetched from MT5")
        return None

    # Convert to DataFrame
    data = pd.DataFrame(rates)
    data['time'] = pd.to_datetime(data['time'], unit='s')
    return data

def main():
    # MT5 settings
    symbol = "EURUSD"  # Change to your desired symbol
    timeframe = mt5.TIMEFRAME_M1  # 1-minute timeframe
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=10000)  # Past 10,000 hours

    # Fetch data from MT5
    data = fetch_data_from_mt5(symbol, timeframe, start_time, end_time)
    if data is None:
        return

    # Filter data to include only the required minutes (58, 59, 00, 01, 02)
    filtered_data = []
    for index, row in data.iterrows():
        if row['time'].minute in [58, 59, 0, 1, 2]:
            filtered_data.append((row['open'], row['close'], row['high'], row['low']))

    conn = connect_to_db()
    create_table(conn)

    patterns = analyze_hourly_data(filtered_data)
    save_to_db(conn, patterns)

    calculate_bull_bear_ratio(conn)

    conn.close()

if __name__ == '__main__':
    main()