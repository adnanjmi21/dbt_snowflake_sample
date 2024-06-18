{{ config(materialized='incremental',unique_key='customerid',incremental_strategy='merge') }}

with raw_customers as (
select * from {{source('tm_raw','customers')}} 

{% if is_incremental() %}

where
  ingested_at > (select max(ingested_at) from {{ this }})

{% endif %} 
)

select * from raw_customers