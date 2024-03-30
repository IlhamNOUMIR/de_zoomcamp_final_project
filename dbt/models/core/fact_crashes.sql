{{ config(
    partition_by={
        "field": "crash_datetime",
        "data_type": "timestamp",
        "granularity": "year"
    },
    cluster_by=["borough"]
) }}



with source as (
    select 
        collision_id,
        crash_datetime,
        borough,
        zip_code,
        latitude,
        longitude,
        location,
        on_street_name,
        number_of_persons_injured,
        number_of_persons_killed,
        contributing_factor_vehicle_1
    from `complete-land-417013`.`dbt_inoumir`.`staging_crashes`
)


select 
    EXTRACT(YEAR FROM crash_datetime) as year,
    borough,
    MAX(contributing_factor_vehicle_1) AS most_common_contributing_factor,
    count(*) as total_accidents,
    sum(number_of_persons_injured) as total_persons_injured,
    sum(number_of_persons_killed) as total_persons_killed
from source
group by 
    year,
    borough