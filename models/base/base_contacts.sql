with base_contacts as (

    select _airbyte_data
    from {{ source('raw_table', '_airbyte_raw_contacts') }}

)

select * from base_contacts