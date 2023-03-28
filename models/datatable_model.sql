{{ config(materialized='table') }}

with union_tables as (
    select *
    from {{ ref('base_table1') }}
)

, tables as (
    select *
    from {{ ref('base_table2') }}
)

select *
from union_tables
union all
select *
from tables