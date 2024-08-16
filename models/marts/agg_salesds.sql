with
    stg_salesorderheader as (
        select *
        from {{ ref('stg_sap__salesorderheader') }}
    )
    , stg_salesorderdetail as (
        select *
        from {{ ref('stg_sap__salesorderdetail') }}
    )
    , stg_store as (
        select *
        from {{ ref('stg_sap__store') }}
    )
    , stg_businessentityaddress as (
        select *
        from {{ ref('stg_sap__businessentityaddress') }}
    )
    , dim_products as (
        select *
        from {{ ref('dim_products') }}
    )
    , stg_territory as (
        select *
        from {{ ref('stg_sap__salesterritory') }}
    )
    , fct_salesdetails as (
        select
            stg_salesorderdetail.orderdetail_id
            , stg_salesorderheader.order_id
            , dim_products.productname as product_name
            , dim_products.productsubcategoryname
            , stg_salesorderheader.customer_id
            , stg_territory.region
            , stg_store.store_name
            , stg_salesorderheader.order_date
            , stg_salesorderdetail.order_qty
            , stg_salesorderdetail.unit_price
            , stg_salesorderdetail.discount as unitprice_discount
            , stg_salesorderheader.total
            , cast(unit_price * order_qty as numeric) as total_details
        from stg_salesorderdetail
        left join stg_salesorderheader on stg_salesorderdetail.order_id = stg_salesorderheader.order_id
        left join stg_businessentityaddress on stg_salesorderheader.shiptoaddress_id = stg_businessentityaddress.address_id
        left join stg_store on stg_businessentityaddress.businessentity_id = stg_store.businessentity_id
        left join dim_products on stg_salesorderdetail.product_id = dim_products.product_id
        left join stg_territory on stg_salesorderheader.territory_id = stg_territory.territory_id
    )
select *
from fct_salesdetails