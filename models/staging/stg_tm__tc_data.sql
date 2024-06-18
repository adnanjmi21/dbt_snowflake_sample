{{ config(materialized='incremental',unique_key='trans_id',incremental_strategy='merge') }}

with raw_tc_data as (
select * from {{source('tm_raw','tc_data')}} 

{% if is_incremental() %}

where
  createdat > (select max(createdat) from {{ this }})

{% endif %} 
)

select * from raw_tc_data