from clickhouse_driver import Client


CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "port": 9000,
    "user": "admin",
    "password": "password"
}


def create_tables():
    """Create required tables in ClickHouse"""
    
    client = Client(**CLICKHOUSE_CONFIG)
    
    client.execute('CREATE DATABASE IF NOT EXISTS weather')
    print("Database 'weather' created/verified")
    
    client.execute('''
        CREATE TABLE IF NOT EXISTS weather.weather_hourly
        (
            city String,
            date Date,
            hour DateTime,
            temperature Float32,
            precipitation Float32,
            wind_speed Float32,
            wind_direction Int32,
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (city, date, hour)
    ''')
    print("Table 'weather_hourly' created/verified")
    
    client.execute('''
        CREATE TABLE IF NOT EXISTS weather.weather_daily
        (
            city String,
            date Date,
            temp_min Float32,
            temp_max Float32,
            temp_avg Float32,
            precipitation_total Float32,
            wind_max Float32,
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (city, date)
    ''')
    print("Table 'weather_daily' created/verified")
    
    print("All tables created successfully")


if __name__ == "__main__":
    create_tables()