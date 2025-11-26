from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from weather_etl import weather_etl_flow


def create_deployment():
    """Create deployment with schedule"""
    
    deployment = Deployment.build_from_flow(
        flow=weather_etl_flow,
        name="weather-etl-production",
        schedule=CronSchedule(cron="* * * * *"),#Настроил на каждую минуту для более простой проверки
        work_pool_name="default-pool",
        parameters={},
        tags=["weather", "etl", "production"]
    )
    
    deployment.apply()
    
    print("Deployment created successfully")


if __name__ == "__main__":
    create_deployment()