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
    , store as (
        select *
        from {{ ref('stg_sap__store') }}
    )
    , dim_clients as (
        select
            {{ numeric_surrogate_key([
                'customer.customer_id'
            ]) }} as customer_sk
            , customer.customer_id
            , customer.person_id
            , creditcard.creditcard_id
            , customer.store_id
            , store.store_name
            , person.full_name
            , creditcard.cardtype
        from customer
        left join person on customer.person_id = person.businessentity_id
        left join personcreditcard on person.businessentity_id = personcreditcard.businessentity_id
        left join creditcard on personcreditcard.creditcard_id = creditcard.creditcard_id
        left join store on customer.store_id = store.businessentity_id
    )
select *
from dim_clients