with
    person as (
        select *
        from {{ ref('stg_sap__person') }}
    )
    , salesperson as (
        select *
        from {{ ref('stg_sap__salesperson') }}
    )
    , fct_sales as (
        select
            salesperson_id
            , count(distinct order_id) as orders_qty
            , sum(total) as total_sales
            , avg(total_sales) as avg_ticket
            , min(order_date) as first_sale
            , max(order_date) as last_sale
        from {{ ref('fct_salesdetails') }}
        group by salesperson_id
    )
    , sales_2013 as (
        select
            salesperson_id
            , sum(total_sales) as total_sales_2013
        from {{ ref('fct_salesdetails') }}
        where extract(year from order_date) = 2013
        group by salesperson_id
    )
    , sales_2014 as (
        select
            salesperson_id
            , sum(total_sales) as total_sales_2014
        from {{ ref('fct_salesdetails') }}
        where extract(year from order_date) = 2014
        group by salesperson_id
    )
    , agg_salesperson as (
        select
            salesperson.person_id
            , person.full_name
            , salesperson.salesquota
            , salesperson.bonus
            , salesperson.commissionpct
            , sales_2013.total_sales_2013
            , sales_2014.total_sales_2014
            , fct_sales.total_sales
            , fct_sales.orders_qty
            , fct_sales.avg_ticket
            , fct_sales.first_sale
            , fct_sales.last_sale
        from salesperson
        left join person on salesperson.person_id = person.businessentity_id
        left join fct_sales on salesperson.person_id = fct_sales.salesperson_id
        left join sales_2014 on salesperson.person_id = sales_2014.salesperson_id
        left join sales_2013 on salesperson.person_id = sales_2013.salesperson_id    
    )
select *
from agg_salesperson