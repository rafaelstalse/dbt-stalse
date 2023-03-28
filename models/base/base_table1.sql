with base_table1 as (

    select *
    from {{ source('raw_table', 'table1') }}

)

select * from base_table1