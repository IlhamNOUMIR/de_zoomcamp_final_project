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
        contributing_factor_vehicle_1,
        contributing_factor_vehicle_2,
        vehicle_type_code_1,
        vehicle_type_code_2
    from {{ source('staging', 'crashes') }}
)

select * from source
