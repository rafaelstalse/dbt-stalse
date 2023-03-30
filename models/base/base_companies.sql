with base_companies as (

    select _airbyte_data
    from {{ source('raw_table', '_airbyte_raw_companies') }}

)

select * from base_companies