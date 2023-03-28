with base_table2 as (

    select *
    from {{ source('raw_table', 'table2') }}

)

select * from base_table2