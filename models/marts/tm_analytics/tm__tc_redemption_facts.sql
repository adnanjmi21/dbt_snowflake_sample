{{ config(materialized='table',full_refresh=true,pre_hook={
"sql": "CREATE OR REPLACE SEQUENCE tm_redemption_facts_sequence START WITH 1 INCREMENT BY 1;"
}) }}


select tm_redemption_facts_sequence.nextval::BIGINT as tm_redemption_facts_id ,*
from( 
select
tc.customerid ,c.firstname customer_name ,tc.tctype , sum(tc.amount) total_amount 
from {{ref('stg_tm__tc_data')}} tc
left join {{ref('stg_tm__customers')}} c
on tc.customerid = c.customerid
group by 1,2,3
)