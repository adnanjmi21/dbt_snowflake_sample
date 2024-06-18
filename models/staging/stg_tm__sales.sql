{{ config(materialized='incremental',unique_key='orderid',incremental_strategy='merge',schema='tm_stage') }}

with raw_sales as (
select * from {{source('tm_raw','sales')}} 

{% if is_incremental() %}

where
  ingested_at > (select max(ingested_at) from {{ this }})

{% endif %} 
)

select * from raw_sales