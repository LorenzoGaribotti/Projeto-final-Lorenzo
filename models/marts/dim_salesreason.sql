with
    salesreason as (
        select *
        from {{ ref('stg_sap__salesreason') }}
    )
    , salesorderheadersalesreason as (
        select *
        from {{ ref('stg_sap__salesorderheadersalesreason') }}
    )
    , dim_salesreason as (
        select
            {{ numeric_surrogate_key([
                'salesorderheadersalesreason.order_id'
                , 'salesorderheadersalesreason.reason_id'
            ]) }} as salesreason_sk
            , salesorderheadersalesreason.order_id
            , salesorderheadersalesreason.reason_id
            , salesreason.reason
            , salesreason.reason_type
        from salesorderheadersalesreason
        left join salesreason on salesorderheadersalesreason.reason_id = salesreason.reason_id
    )
select *
from dim_salesreason