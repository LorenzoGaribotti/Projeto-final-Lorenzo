with
    customer as (
        select *
        from {{ ref('stg_sap__customer') }}
    )
    , person as (
        select *
        from {{ ref('stg_sap__person') }}
    )
    , personcreditcard as (
        select *
        from {{ ref('stg_sap__personcreditcard') }}
    )
    , creditcard as (
        select *
        from {{ ref('stg_sap__creditcard') }}
    )
    , dim_clients as (
        select
            {{ numeric_surrogate_key([
                'customer.customer_id'
            ]) }} as customer_sk
            , customer.customer_id
            , customer.person_id
            , creditcard.creditcard_id
            , person.full_name
            , creditcard.cardtype
        from customer
        left join person on customer.person_id = person.businessentity_id
        left join personcreditcard on person.businessentity_id = personcreditcard.businessentity_id
        left join creditcard on personcreditcard.creditcard_id = creditcard.creditcard_id
    )
select *
from dim_clients