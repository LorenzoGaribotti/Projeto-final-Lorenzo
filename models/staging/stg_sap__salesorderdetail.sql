with
    source_salesorderdetail as (
        select
            cast(salesorderdetailid as int) as orderdetail_id,
            cast(salesorderid as int) as order_id,
            cast(productid as int) as product_id,
            cast(specialofferid as int) as offer_id,
            cast(orderqty as int) as order_qty,
            cast(unitprice as numeric) as unit_price,
            cast(unitpricediscount as numeric) as discount
        from {{ source('source', 'salesorderdetail') }}
    )
select *
from source_salesorderdetail
