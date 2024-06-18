{{ config(materialized='incremental',unique_key='orderid',incremental_strategy='merge',pre_hook={
"sql": "CREATE OR REPLACE SEQUENCE tm_customer_sales_sequence START WITH 1 INCREMENT BY 1;"
}) }}



with customer_sales as (
select 
s.orderid ,s.customerid,c.firstname,s.prediscountgrossproductsales,s.orderweight, s.ingested_at  
from {{ref('stg_tm__sales')}} s
left join {{ref('stg_tm__customers')}} c
on s.customerid=c.customerid

{% if is_incremental() %}

where
  s.ingested_at > (select max(ingested_at) from {{ this }})

{% endif %} 
)

select tm_customer_sales_sequence.nextval::BIGINT as tm_customers_sales_id, * from customer_sales