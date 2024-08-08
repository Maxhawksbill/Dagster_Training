from dagster import Definitions, load_assets_from_modules
from .resources import database_resource
from .jobs import trip_update_job, weekly_update_job, adhoc_request_job
from .schedules import trip_update_schedule, weekly_update_schedule
from .sensors import adhoc_request_sensor
from .assets import metrics, trips, requests

trip_assets = load_assets_from_modules(modules=[trips], group_name='raw_file')
metric_assets = load_assets_from_modules(modules=[metrics], group_name='metrics',)
all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]
request_assets = load_assets_from_modules(modules=[requests], group_name='requests')
all_sensors = [adhoc_request_sensor]

defs = Definitions(
    assets=[*trip_assets, *metric_assets, *request_assets],
    resources={
        "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)

