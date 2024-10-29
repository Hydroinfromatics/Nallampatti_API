#!/usr/bin/env python3

import requests
import pandas as pd
from datetime import datetime, time, timedelta
import json
import time as time_module
import sys
import os
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
    global data_store
    if data_store.empty:
        return pd.DataFrame()
    
    start_date = datetime.now().date() - timedelta(days=days)
    historical_data = data_store[data_store['timestamp'].dt.date >= start_date]
    return historical_data

def format_data_as_json(df, data_type="live"):
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
    global data_store
    
    print(f"Starting continuous monitoring at {datetime.now()}")
    print(f"Update interval: {update_interval} seconds")
    
    try:
        while True:
            try:
                data = fetch_data_from_api(api_url)
                if data:
                    new_data = process_data(data)
                    if not new_data.empty:
                        data_store = pd.concat([data_store, new_data]).drop_duplicates().sort_values('timestamp')
                        
                        historical_data = get_historical_data(days=7)
                        historical_json = format_data_as_json(historical_data, "historical")
                        
                        live_data = get_historical_data(days=1)
                        live_json = format_data_as_json(live_data, "live")
                        
                        # Clear console for better readability
                        os.system('cls' if os.name == 'nt' else 'clear')
                        
                        print("\n=== Historical Data (Last 7 Days) ===")
                        print(json.dumps(historical_json, indent=2))
                        
                        print("\n=== Live Data (Last 24 Hours) ===")
                        print(json.dumps(live_json, indent=2))
                        
                        print(f"\nLast updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"Next update in {update_interval} seconds...")
                
                time_module.sleep(update_interval)
                
            except requests.exceptions.RequestException as e:
                print(f"Network error occurred: {e}")
                print("Retrying in 30 seconds...")
                time_module.sleep(30)
            except Exception as e:
                print(f"Error during monitoring: {e}")
                print("Retrying in 30 seconds...")
                time_module.sleep(30)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")
        sys.exit(0)

def main():
    try:
        # Get API URL from environment variable or use default
        api_url = os.getenv('API_URL', 'YOUR_API_URL_HERE')
        update_interval = int(os.getenv('UPDATE_INTERVAL', '60'))
        
        if api_url == 'YOUR_API_URL_HERE':
            print("Error: API_URL not set. Please set the API_URL environment variable.")
            sys.exit(1)
            
        continuous_monitoring(api_url, update_interval)
        
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
