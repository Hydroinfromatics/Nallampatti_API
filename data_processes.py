import requests
import pandas as pd
from datetime import datetime, time, timedelta
import json
import time as time_module
from get_data import fetch_data_from_api

# Global variable to store the data
data_store = pd.DataFrame()

def process_data(data):
    if data is None:
        print("No data received from API")
        return pd.DataFrame()  
    try:
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            print(f"Unexpected data format. Expected list or dict, got {type(data)}")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        if df.empty:
            print("DataFrame is empty after conversion")
            return df

        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%d-%b-%Y %H:%M:%S', errors='coerce')
        
        required_columns = ['timestamp', 'pH', 'TDS', 'Depth', 'FlowInd']
        for col in required_columns:
            if col not in df.columns:
                print(f"Missing column: {col}")
                df[col] = None
        
        return df.sort_values('timestamp')
    except Exception as e:
        print(f"Error processing data: {e}")
        return pd.DataFrame()

def get_historical_data(days=7):
    """Get data from the past specified number of days"""
    global data_store
    if data_store.empty:
        return pd.DataFrame()
    
    start_date = datetime.now().date() - timedelta(days=days)
    historical_data = data_store[data_store['timestamp'].dt.date >= start_date]
    return historical_data

def format_data_as_json(df, data_type="live"):
    """Convert DataFrame to JSON with specified data type"""
    try:
        df_copy = df.copy()
        df_copy['timestamp'] = df_copy['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        json_data = {
            "status": "success",
            "data_type": data_type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "record_count": len(df_copy),
            "data": json.loads(df_copy.to_json(orient='records'))
        }
        return json_data
    except Exception as e:
        return {
            "status": "error",
            "data_type": data_type,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "error": str(e)
        }

def continuous_monitoring(api_url, update_interval=60):
    """Continuously monitor and display both historical and live data"""
    global data_store
    
    print("Starting continuous monitoring...")
    print(f"Update interval: {update_interval} seconds")
    
    try:
        while True:
            # Fetch and process new data
            data = fetch_data_from_api(api_url)
            if data:
                new_data = process_data(data)
                if not new_data.empty:
                    data_store = pd.concat([data_store, new_data]).drop_duplicates().sort_values('timestamp')
                    
                    # Get and display historical data
                    historical_data = get_historical_data(days=7)
                    historical_json = format_data_as_json(historical_data, "historical")
                    
                    # Get and display live data (last 24 hours)
                    live_data = get_historical_data(days=1)
                    live_json = format_data_as_json(live_data, "live")
                    
                    # Clear console for better readability
                    print("\033c", end='')
                    
                    # Display both data sets
                    print("\n=== Historical Data (Last 7 Days) ===")
                    print(json.dumps(historical_json, indent=2))
                    
                    print("\n=== Live Data (Last 24 Hours) ===")
                    print(json.dumps(live_json, indent=2))
                    
                    print(f"\nLast updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"Next update in {update_interval} seconds...")
            
            # Wait for the specified interval
            time_module.sleep(update_interval)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    api_url = "https://mongodb-api-hmeu.onrender.com"   # Replace with your actual API URL
    
    # Start continuous monitoring with 60-second updates
    continuous_monitoring(api_url, update_interval=600)