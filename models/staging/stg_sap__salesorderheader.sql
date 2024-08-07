with
    source_salesorderheader as (
        select
            cast(salesorderid as int) as order_id
            , cast(customerid as int) as customer_id
            , cast(territoryid as int) as territory_id
            , cast(creditcardid as int) as creditcard_id
            , cast(shiptoaddressid as int) as shiptoaddress_id
            , cast(salespersonid as int) as salesperson_id
            , date(orderdate) as order_date
            , date(duedate) as due_date
            , date(shipdate) as ship_date
            , cast(status as int) as status
            , cast(subtotal as numeric) as subtotal
            , cast(taxamt as numeric) as tax
            , cast(freight as numeric) as freight
            , cast(totaldue as numeric) as total
    from {{ source('source', 'salesorderheader') }}
    )
select *
from source_salesorderheader