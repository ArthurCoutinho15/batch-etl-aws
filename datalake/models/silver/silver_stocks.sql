{{config(materialized='table')}}

with source as (
    select *
    from {{ source('finance_db', 'bronze_stocks')}}
)

select *
from source