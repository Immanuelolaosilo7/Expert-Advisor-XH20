import MetaTrader5 as mt5
import pandas as pd
import pymysql
from datetime import datetime, timedelta

# Initialize MT5 connection
mt5.initialize()

# Database connection details
db_host = 'localhost'
db_user = 'Mudang'
db_password = '*'
db_name = 'your_database_name'  # Replace with your actual database name

# Connect to MySQL database
connection = pymysql.connect(host=db_host, user=db_user, password=db_password, database=db_name)
cursor = connection.cursor()

# Function to fetch candlestick data from MT5
def fetch_candles(symbol, timeframe, start, end):
    rates = mt5.copy_rates_range(symbol, timeframe, start, end)
    return pd.DataFrame(rates)

# Function to calculate pipettes
def calculate_pipettes(open_price, close_price, high_price, low_price):
    body = abs(close_price - open_price)
    upper_wick = high_price - max(open_price, close_price)
    lower_wick = min(open_price, close_price) - low_price
    longer_wick = max(upper_wick, lower_wick)
    shorter_wick = min(upper_wick, lower_wick)
    return body, longer_wick, shorter_wick

# Function to identify candlestick pattern
def identify_pattern(body, longer_wick, shorter_wick, upper_wick, lower_wick):
    if body > longer_wick and body > shorter_wick:
        if longer_wick == upper_wick:
            return 'Pattern1'
        else:
            return 'Pattern2'
    elif body < longer_wick and body < shorter_wick:
        if longer_wick == upper_wick:
            return 'Pattern3'
        else:
            return 'Pattern4'
    elif body < longer_wick and shorter_wick < body:
        if longer_wick == upper_wick:
            return 'Pattern5'
        else:
            return 'Pattern6'
    return 'Unknown'

# Function to check if the pattern meets the conditions
def meets_conditions(body, longer_wick, shorter_wick, min_longer_wick, min_diff_body_shorter, min_diff_longer_shorter, min_diff_longer_body):
    # Longer wick must meet minimum pipettes
    if longer_wick < min_longer_wick:
        return False
    
    # Difference between body and shorter wick must meet minimum pipettes (can be zero)
    if abs(body - shorter_wick) < min_diff_body_shorter:
        return False
    
    # Difference between longer wick and shorter wick must meet minimum pipettes
    if abs(longer_wick - shorter_wick) < min_diff_longer_shorter:
        return False
    
    # Difference between longer wick and body must meet minimum pipettes
    if abs(longer_wick - body) < min_diff_longer_body:
        return False
    
    return True

# Function to determine if the three-minute sequence is bullish or bearish
def is_bullish_sequence(candle59, candle01, min_pipettes_3min):
    price_diff = abs(candle01['close'] - candle59['open'])
    return price_diff >= min_pipettes_3min and candle01['close'] > candle59['open']

# Function to determine if a time interval is bullish or bearish
def is_bullish_interval(candle_start, candle_end):
    return candle_end['close'] > candle_start['open']

# Function to check if the price movement meets the minimum pipettes requirement
def meets_min_pipettes(candle_start, candle_end, min_pipettes):
    price_diff = abs(candle_end['close'] - candle_start['open'])
    return price_diff >= min_pipettes

# Main function to process data
def process_data(symbol, timeframe, min_longer_wick, min_diff_body_shorter, min_diff_longer_shorter, min_diff_longer_body, min_pipettes_3min, min_pipettes_intervals):
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1000)
    
    candles = fetch_candles(symbol, timeframe, start_time, end_time)
    candles['time'] = pd.to_datetime(candles['time'], unit='s')
    
    # Group candles by hour
    candles['hour'] = candles['time'].dt.floor('H')
    grouped = candles.groupby('hour')
    
    pattern_data = {}
    
    for hour, group in grouped:
        # Ensure we have candles at 59, 00, and 01 minutes
        if len(group) >= 3:
            candle59 = group.iloc[0]  # 59th minute
            candle00 = group.iloc[1]  # 00th minute
            candle01 = group.iloc[2]  # 01st minute
            
            # Calculate pipettes for each candle
            body59, longer_wick59, shorter_wick59 = calculate_pipettes(candle59['open'], candle59['close'], candle59['high'], candle59['low'])
            body00, longer_wick00, shorter_wick00 = calculate_pipettes(candle00['open'], candle00['close'], candle00['high'], candle00['low'])
            body01, longer_wick01, shorter_wick01 = calculate_pipettes(candle01['open'], candle01['close'], candle01['high'], candle01['low'])
            
            # Check if all three candles meet the conditions
            if meets_conditions(body59, longer_wick59, shorter_wick59, min_longer_wick, min_diff_body_shorter, min_diff_longer_shorter, min_diff_longer_body) and \
               meets_conditions(body00, longer_wick00, shorter_wick00, min_longer_wick, min_diff_body_shorter, min_diff_longer_shorter, min_diff_longer_body) and \
               meets_conditions(body01, longer_wick01, shorter_wick01, min_longer_wick, min_diff_body_shorter, min_diff_longer_shorter, min_diff_longer_body):
                
                # Identify patterns for each candle
                pattern59 = identify_pattern(body59, longer_wick59, shorter_wick59, candle59['high'] - max(candle59['open'], candle59['close']), min(candle59['open'], candle59['close']) - candle59['low'])
                pattern00 = identify_pattern(body00, longer_wick00, shorter_wick00, candle00['high'] - max(candle00['open'], candle00['close']), min(candle00['open'], candle00['close']) - candle00['low'])
                pattern01 = identify_pattern(body01, longer_wick01, shorter_wick01, candle01['high'] - max(candle01['open'], candle01['close']), min(candle01['open'], candle01['close']) - candle01['low'])
                
                # Combine patterns for the hour
                combined_pattern = f"{pattern59}_{pattern00}_{pattern01}"
                
                # Determine if the three-minute sequence is bullish or bearish
                is_bullish_sequence_result = is_bullish_sequence(candle59, candle01, min_pipettes_3min)
                three_minute_trend = 'buy' if is_bullish_sequence_result else 'sell'
                
                # Determine trends for different intervals
                intervals = {
                    'full_hour': (0, -1, min_pipettes_intervals['full_hour']),  # 00:00 to 59:59
                    'first_10_min': (0, 9, min_pipettes_intervals['first_10_min']),  # 00:00 to 09:59
                    'first_15_min': (0, 14, min_pipettes_intervals['first_15_min']),  # 00:00 to 14:59
                    'first_20_min': (0, 19, min_pipettes_intervals['first_20_min']),  # 00:00 to 19:59
                    'first_30_min': (0, 29, min_pipettes_intervals['first_30_min'])   # 00:00 to 29:59
                }
                
                # Create a unique identifier combining pattern and 3-minute trend
                unique_identifier = f"{combined_pattern}_{three_minute_trend}"
                
                # Initialize pattern data if not already present
                if unique_identifier not in pattern_data:
                    pattern_data[unique_identifier] = {
                        'full_hour': {'bull_count': 0, 'bear_count': 0},
                        'first_10_min': {'bull_count': 0, 'bear_count': 0},
                        'first_15_min': {'bull_count': 0, 'bear_count': 0},
                        'first_20_min': {'bull_count': 0, 'bear_count': 0},
                        'first_30_min': {'bull_count': 0, 'bear_count': 0}
                    }
                
                # Update counts for each interval
                for interval, (start_idx, end_idx, min_pipettes) in intervals.items():
                    candle_start = group.iloc[start_idx]
                    candle_end = group.iloc[end_idx]
                    
                    # Check if the interval meets the minimum pipettes requirement
                    if meets_min_pipettes(candle_start, candle_end, min_pipettes):
                        is_bullish = is_bullish_interval(candle_start, candle_end)
                        
                        if is_bullish:
                            pattern_data[unique_identifier][interval]['bull_count'] += 1
                        else:
                            pattern_data[unique_identifier][interval]['bear_count'] += 1
    
    return pattern_data

# Function to save patterns to MySQL
def save_patterns_to_db(pattern_data):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS candlestick_patterns (
        id INT AUTO_INCREMENT PRIMARY KEY,
        pattern_name VARCHAR(255),
        three_minute_trend VARCHAR(50),
        interval_type VARCHAR(50),
        bull_count INT,
        bear_count INT,
        bull_ratio FLOAT,
        bear_ratio FLOAT
    )
    """
    cursor.execute(create_table_query)
    
    for unique_identifier, data in pattern_data.items():
        pattern_name, three_minute_trend = unique_identifier.rsplit('_', 1)
        
        for interval_type, counts in data.items():
            bull_count = counts['bull_count']
            bear_count = counts['bear_count']
            total = bull_count + bear_count
            bull_ratio = (bull_count / total) * 100 if total > 0 else 0
            bear_ratio = (bear_count / total) * 100 if total > 0 else 0
            
            insert_query = """
            INSERT INTO candlestick_patterns (pattern_name, three_minute_trend, interval_type, bull_count, bear_count, bull_ratio, bear_ratio)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (pattern_name, three_minute_trend, interval_type, bull_count, bear_count, bull_ratio, bear_ratio))
    
    connection.commit()

# Main execution
if __name__ == "__main__":
    symbol = 'EURUSD'
    timeframe = mt5.TIMEFRAME_M1
    min_longer_wick = 5  # Example minimum pipettes for longer wick
    min_diff_body_shorter = 0  # Example minimum difference between body and shorter wick
    min_diff_longer_shorter = 3  # Example minimum difference between longer and shorter wick
    min_diff_longer_body = 2  # Example minimum difference between longer wick and body
    min_pipettes_3min = 5  # Example minimum pipettes for 3-minute trend
    
    # Minimum pipettes requirements for intervals
    min_pipettes_intervals = {
        'full_hour': 10,  # Minimum pipettes for full hour
        'first_10_min': 2,  # Minimum pipettes for first 10 minutes
        'first_15_min': 3,  # Minimum pipettes for first 15 minutes
        'first_20_min': 4,  # Minimum pipettes for first 20 minutes
        'first_30_min': 5   # Minimum pipettes for first 30 minutes
    }
    
    pattern_data = process_data(symbol, timeframe, min_longer_wick, min_diff_body_shorter, min_diff_longer_shorter, min_diff_longer_body, min_pipettes_3min, min_pipettes_intervals)
    save_patterns_to_db(pattern_data)
    
    cursor.close()
    connection.close()
    mt5.shutdown()