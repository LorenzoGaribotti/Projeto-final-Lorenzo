with
    dim_location as (
        select *
        from {{ ref('dim_location') }}
    )
    , stg_salesorderheader as (
        select *
        from {{ ref('stg_sap__salesorderheader') }}
    )
    , stg_salesorderdetail as (
        select *
        from {{ ref('stg_sap__salesorderdetail') }}
    )
    , fct_salesdetails as (
        select
            {{ numeric_surrogate_key([
                'stg_salesorderdetail.orderdetail_id'
            ]) }} as orderdetail_sk
            , stg_salesorderdetail.orderdetail_id
            , stg_salesorderheader.order_id
            , stg_salesorderdetail.product_id
            , stg_salesorderheader.creditcard_id
            , stg_salesorderheader.customer_id
            , stg_salesorderheader.shiptoaddress_id
            , dim_location.stateprovince_id
            , dim_location.country_code
            , stg_salesorderheader.status
            , stg_salesorderheader.order_date
            , stg_salesorderdetail.order_qty
            , stg_salesorderdetail.unit_price
            , stg_salesorderdetail.discount as unitprice_discount
            , stg_salesorderheader.total
            , cast(unit_price * order_qty as numeric) as total_sales
        from stg_salesorderdetail
        left join stg_salesorderheader on stg_salesorderdetail.order_id = stg_salesorderheader.order_id
        left join dim_location on stg_salesorderheader.shiptoaddress_id = dim_location.address_id
    )
select *
from fct_salesdetails