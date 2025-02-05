import MetaTrader5 as mt5
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from datetime import datetime

# Connect to MT5
def connect_to_mt5():
    if not mt5.initialize():
        print("Failed to initialize MT5")
        return False
    return True

# Fetch candlestick data from MT5
def fetch_candlestick_data(pair, timeframe, num_candles):
    rates = mt5.copy_rates_from_pos(pair, timeframe, 0, num_candles)
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    return df

# Connect to MySQL
def connect_to_mysql():
    engine = create_engine("mysql+mysqlconnector://Emmanuel:Password@localhost/Blue")
    return engine

# Save data to MySQL
def save_to_mysql(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Assign numbers to groups based on hardcoded sets
def assign_decimal_group(number):
    G1 = {1, 2, 3, 4}
    G2 = {5, 6, 7, 8, 9, 15, 16, 17, 18, 19, 25, 26, 27, 28, 29, 35, 36, 37, 38, 39, 45, 46, 47, 48, 49}
    G3 = {12, 13, 14, 23, 24, 34}
    G4 = {10, 20, 21, 30, 31, 32, 40, 41, 42, 43}
    G5 = {11, 22, 33, 44}
    G6 = {55, 66, 77, 88, 99}
    G7 = {50, 51, 52, 53, 54, 60, 61, 62, 63, 64, 70, 71, 72, 73, 74, 80, 81, 82, 83, 84, 90, 91, 92, 93, 94}
    G8 = {56, 57, 58, 59, 65, 67, 68, 69, 75, 76, 78, 79, 85, 86, 87, 89, 95, 96, 97, 98}

    if number in G1:
        return "G1"
    elif number in G2:
        return "G2"				
    elif number in G3:
        return "G3"
    elif number in G4:
        return "G4"
    elif number in G5:
        return "G5"
    elif number in G6:
        return "G6"
    elif number in G7:
        return "G7"
    elif number in G8:
        return "G8"
    else:
        return "Unknown"

# Extract decimal part and analyze tens/units
def analyze_decimal_part(price):
    decimal_part = int((price - int(price)) * 100)  # Extract two decimal digits
    return decimal_part

# Detect crossings in integer part
def detect_crossings(prev_price, curr_price):
    crossings = 0
    prev_int = int(prev_price)
    curr_int = int(curr_price)
    if prev_int < curr_int:
        crossings = curr_int - prev_int
    elif prev_int > curr_int:
        crossings = prev_int - curr_int
    return f"C{crossings}"

# Assign identifier names for tens and units
def assign_identifier_names(prices):
    tens_list = []
    units_list = []
    for price in prices:
        decimal_part = analyze_decimal_part(price)
        tens = decimal_part // 10  # Tens digit
        units = decimal_part % 10  # Units digit
        tens_list.append(tens)
        units_list.append(units)
    
    # Sort tens and units in descending order and assign identifiers
    sorted_tens = sorted(set(tens_list), reverse=True)
    sorted_units = sorted(set(units_list), reverse=True)
    
    tens_identifiers = {num: f"T{idx+1}" for idx, num in enumerate(sorted_tens)}
    units_identifiers = {num: f"U{idx+1}" for idx, num in enumerate(sorted_units)}
    
    return tens_identifiers, units_identifiers

# Assign identifier names for crossings
def assign_crossing_identifiers(crossings):
    unique_crossings = sorted(set(crossings), reverse=True)
    crossing_identifiers = {crossing: f"X{idx+1}" for idx, crossing in enumerate(unique_crossings)}
    return crossing_identifiers

# Analyze matching tens/units
def analyze_matching_tens_units(prices):
    tens_list = []
    units_list = []
    for price in prices:
        decimal_part = analyze_decimal_part(price)
        tens = decimal_part // 10  # Tens digit
        units = decimal_part % 10  # Units digit
        tens_list.append(tens)
        units_list.append(units)
    
    tens_count = {}
    units_count = {}
    for tens in tens_list:
        tens_count[tens] = tens_count.get(tens, 0) + 1
    for units in units_list:
        units_count[units] = units_count.get(units, 0) + 1
    
    return tens_count, units_count

# Assign pattern name
def assign_pattern_name(crossings, decimal_group, greater_less, tens_count, units_count, tens_identifiers, units_identifiers, crossing_identifiers):
    pattern_name = f"{crossings}_{decimal_group}_{greater_less}"
    if tens_count:
        pattern_name += "_T" + "_".join([f"{tens_identifiers[k]}{v}" for k, v in tens_count.items()])
    if units_count:
        pattern_name += "_U" + "_".join([f"{units_identifiers[k]}{v}" for k, v in units_count.items()])
    if crossings:
        pattern_name += "_" + crossing_identifiers[crossings]
    return pattern_name

# Analyze successor candle (bull/bear) and calculate pip difference
def analyze_successor_candle(df):
    df['successor_bull'] = df['close'].shift(-1) > df['open'].shift(-1)
    df['successor_bear'] = df['close'].shift(-1) < df['open'].shift(-1)
    df['successor_ratio'] = df['successor_bull'].astype(int) / (df['successor_bull'].astype(int) + df['successor_bear'].astype(int))
    df['successor_pip'] = (df['close'].shift(-1) - df['open'].shift(-1)) * 10000  # Calculate pip difference
    return df

# Visualize candlestick data
def visualize_candlestick_data(df):
    fig = go.Figure(data=[go.Candlestick(
        x=df['time'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close']
    )])
    fig.update_layout(title="Candlestick Chart", xaxis_title="Time", yaxis_title="Price")
    fig.show()

# Main function
def main():
    # Connect to MT5
    if not connect_to_mt5():
        return

    # Fetch candlestick data
    pair = "XAUUSD"
    timeframe = mt5.TIMEFRAME_M1  # 1-minute timeframe
    num_candles = 50000
    df = fetch_candlestick_data(pair, timeframe, num_candles)

    # Analyze decimal parts and assign groups
    df['decimal_part'] = df['close'].apply(analyze_decimal_part)
    df['group'] = df['decimal_part'].apply(assign_decimal_group)

    # Detect crossings
    df['crossings'] = df['close'].diff().apply(lambda x: detect_crossings(x, x))

    # Assign identifier names for tens, units, and crossings
    tens_identifiers, units_identifiers = assign_identifier_names(df['close'])
    crossing_identifiers = assign_crossing_identifiers(df['crossings'])

    # Analyze matching tens/units
    tens_count, units_count = analyze_matching_tens_units(df['close'])

    # Assign pattern names
    df['pattern_name'] = df.apply(
        lambda row: assign_pattern_name(row['crossings'], row['group'], "GT" if row['decimal_part'] // 10 > row['decimal_part'] % 10 else "LT", tens_count, units_count, tens_identifiers, units_identifiers, crossing_identifiers),
        axis=1
    )

    # Analyze successor candles
    df = analyze_successor_candle(df)

    # Connect to MySQL
    engine = connect_to_mysql()

    # Save candlestick data to MySQL
    save_to_mysql(df, "candlestick_data", engine)

    # Visualize candlestick data
    visualize_candlestick_data(df)

if __name__ == "__main__":
    main()