{{ config(materialized='incremental',unique_key='trans_id',incremental_strategy='merge',pre_hook={
"sql": "CREATE OR REPLACE SEQUENCE tm_tc_redemption_sequence START WITH 1 INCREMENT BY 1;"
}) }}

with spent as (
 SELECT
    TRANS_ID,
    TCTYPE,
    CREATEDAT,
    EXPIREDAT,
    CUSTOMERID,
    ORDERID,
    AMOUNT,
    REASON,
    FIRST_VALUE(CASE WHEN TCTYPE IN ('spent', 'expired') THEN TRANS_ID END) OVER (
            PARTITION BY CUSTOMERID
            ORDER BY CREATEDAT ASC
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED Following
        ) redeem_temp
FROM {{ref('stg_tm__tc_data')}} 
{% if is_incremental() %}

where
  createdat > (select max(createdat) from {{ this }})

{% endif %} 
order by customerid,createdat)

select 
    tm_tc_redemption_sequence.nextval::BIGINT as tm_tc_redemption_id, 
    TRANS_ID,
    TCTYPE,
    CREATEDAT,
    EXPIREDAT,
    CUSTOMERID,
    ORDERID,
    AMOUNT,
    REASON , 
    case 
    when 
    tctype='earned' then first_value(redeem_temp ignore nulls) 
    over(partition by customerid order by createdat asc 
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED Following) end redeemid 
    from spent 
    order by customerid , createdat desc 