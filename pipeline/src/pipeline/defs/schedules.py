from dagster import DefaultScheduleStatus, ScheduleDefinition

from pipeline.defs.jobs import weather_etl_job

weather_etl_schedule = ScheduleDefinition(
    name="weather_etl_schedule",
    job=weather_etl_job,
    cron_schedule="30 23 * * *",
    execution_timezone="Asia/Tokyo",
    default_status=DefaultScheduleStatus.RUNNING,
)
