with source as (
    select 
        borough,
        zip_code,
        latitude,
        longitude,
        on_street_name
    from {{ ref('staging_crashes') }}  
)

select distinct * from source