import requests
from datetime import datetime, timedelta
import json
import io
import os
from typing import Dict, List

from minio import Minio
from clickhouse_driver import Client
from prefect import flow, task


# Configuration
CITIES = {
    "Moscow": {"latitude": 55.7558, "longitude": 37.6173},
    "Samara": {"latitude": 53.1959, "longitude": 50.1002}
}

WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
TELEGRAM_API_URL = "https://api.telegram.org/bot"

# Connection settings
MINIO_CONFIG = {
    "endpoint": "minio:9000",
    "access_key": "minioadmin", 
    "secret_key": "minioadmin",
    "secure": False
}

CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "user": "admin",
    "password": "password",
    "database": "weather"
}


def get_chat_ids(bot_token: str) -> List[str]:
    """Get list of all bot user chat IDs"""
    try:
        url = f"{TELEGRAM_API_URL}{bot_token}/getUpdates"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if not data['ok']:
            return []
        
        chat_ids = {
            str(update['message']['chat']['id']) 
            for update in data['result'] 
            if 'message' in update
        }
        
        print(f"Found {len(chat_ids)} users")
        return list(chat_ids)
        
    except Exception as e:
        print(f"Error getting chat IDs: {e}")
        return []


@task(retries=3, retry_delay_seconds=10)
def fetch_weather_data(city: str, latitude: float, longitude: float) -> Dict:
    """Fetch weather forecast for specified city"""
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "hourly": "temperature_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "timezone": "Europe/Moscow",
        "forecast_days": 2
    }
    
    print(f"Fetching data for {city}")
    
    try:
        response = requests.get(WEATHER_API_URL, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        data.update({
            'city': city,
            'fetched_at': datetime.now().isoformat()
        })
        
        return data
        
    except Exception as e:
        print(f"API error for {city}: {e}")
        raise


@task
def save_raw_data_to_minio(data: Dict, city: str) -> str:
    """Save raw data to MinIO"""
    
    client = Minio(**MINIO_CONFIG)
    
    bucket_name = "weather-raw"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{city}_{timestamp}.json"
    json_data = json.dumps(data, ensure_ascii=False, indent=2)
    
    try:
        client.put_object(
            bucket_name,
            filename,
            data=io.BytesIO(json_data.encode('utf-8')),
            length=len(json_data),
            content_type='application/json'
        )
        
        return filename
        
    except Exception as e:
        print(f"MinIO save error: {e}")
        raise


@task
def transform_hourly_data(data: Dict) -> List[Dict]:
    """Transform hourly data for tomorrow"""
    
    city = data['city']
    hourly_data = data['hourly']
    tomorrow = (datetime.now() + timedelta(days=1)).date()
    
    transformed = [
        {
            'city': city,
            'date': datetime.fromisoformat(hourly_data['time'][i]).date(),
            'hour': datetime.fromisoformat(hourly_data['time'][i]),
            'temperature': hourly_data['temperature_2m'][i],
            'precipitation': hourly_data['precipitation'][i],
            'wind_speed': hourly_data['wind_speed_10m'][i],
            'wind_direction': hourly_data['wind_direction_10m'][i]
        }
        for i in range(len(hourly_data['time']))
        if datetime.fromisoformat(hourly_data['time'][i]).date() == tomorrow
    ]
    
    print(f"Hourly data for {city}: {len(transformed)} records")
    return transformed


@task
def transform_daily_data(data: Dict) -> Dict:
    """Aggregate daily data for tomorrow"""
    
    city = data['city']
    daily_data = data['daily']
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        tomorrow_index = daily_data['time'].index(tomorrow)
        
        temp_min = daily_data['temperature_2m_min'][tomorrow_index]
        temp_max = daily_data['temperature_2m_max'][tomorrow_index]
        temp_avg = round((temp_min + temp_max) / 2, 1)
        
        daily_summary = {
            'city': city,
            'date': datetime.strptime(tomorrow, '%Y-%m-%d').date(),
            'temp_min': temp_min,
            'temp_max': temp_max,
            'temp_avg': temp_avg,
            'precipitation_total': daily_data['precipitation_sum'][tomorrow_index],
            'wind_max': 0
        }
        
        return daily_summary
        
    except (ValueError, IndexError) as e:
        print(f"No data for tomorrow in {city}: {e}")
        raise


@task
def load_hourly_data_to_clickhouse(data: List[Dict]):
    """Load hourly data to ClickHouse"""
    
    if not data:
        print("No hourly data to load")
        return
        
    client = Client(**CLICKHOUSE_CONFIG)
    
    for record in data:
        client.execute(
            "INSERT INTO weather_hourly (city, date, hour, temperature, precipitation, wind_speed, wind_direction) VALUES",
            [record]
        )
    
    print(f"Loaded {len(data)} hourly records")


@task
def load_daily_data_to_clickhouse(data: Dict):
    """Load daily data to ClickHouse"""
    
    client = Client(**CLICKHOUSE_CONFIG)
    client.execute(
        "INSERT INTO weather_daily (city, date, temp_min, temp_max, temp_avg, precipitation_total, wind_max) VALUES",
        [data]
    )


@task
def send_telegram_notifications(daily_data: Dict):
    """Send notifications to all bot users"""
    
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    
    if not bot_token:
        print("Bot token not configured")
        return
    
    chat_ids = get_chat_ids(bot_token)
    
    if not chat_ids:
        print("No users found")
        return
    
    city = daily_data['city']
    
    message = f"""
Weather forecast for tomorrow ({daily_data['date']})
City: {city}

Temperature:
Min: {daily_data['temp_min']}°C
Max: {daily_data['temp_max']}°C  
Avg: {daily_data['temp_avg']}°C

Precipitation: {daily_data['precipitation_total']} mm
"""
    
    successful, failed = 0, 0
    
    for chat_id in chat_ids:
        try:
            url = f"{TELEGRAM_API_URL}{bot_token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message
            }
            
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                successful += 1
            else:
                failed += 1
                
        except Exception:
            failed += 1
    
    print(f"Notifications sent: {successful} successful, {failed} failed")


def process_city(city: str, coords: Dict) -> bool:
    """Process data for one city"""
    try:
        print(f"Processing: {city}")
        
        # Extract
        raw_data = fetch_weather_data(city, coords["latitude"], coords["longitude"])
        
        # Save raw data
        save_raw_data_to_minio(raw_data, city)
        
        # Transform
        hourly_data = transform_hourly_data(raw_data)
        daily_data = transform_daily_data(raw_data)
        
        # Load
        load_hourly_data_to_clickhouse(hourly_data)
        load_daily_data_to_clickhouse(daily_data)
        
        # Notify
        send_telegram_notifications(daily_data)
        
        return True
        
    except Exception as e:
        print(f"Error processing {city}: {e}")
        return False


@flow(name="weather-etl", log_prints=True)
def weather_etl_flow():
    """Main ETL pipeline for weather data collection"""
    
    print("Starting ETL pipeline")
    print(f"Cities: {', '.join(CITIES.keys())}")
    
    success_count = 0
    
    for city, coords in CITIES.items():
        if process_city(city, coords):
            success_count += 1
    
    print(f"Pipeline completed: {success_count}/{len(CITIES)} cities processed")
    print("Data stored in MinIO and ClickHouse")


if __name__ == "__main__":
    weather_etl_flow()