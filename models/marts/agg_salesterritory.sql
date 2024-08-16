with
    salesterritory as (
        select *
        from {{ ref('stg_sap__salesterritory') }}
    )
    , fct_sales as (
        select
            territory_id
            , count(distinct order_id) as orders_qty
            , sum(total) as total_sales
            , avg(total_sales) as avg_ticket
            , min(order_date) as first_order
            , max(order_date) as last_order
        from {{ ref('fct_salesdetails') }}
        group by territory_id
    )
    , sales_2013 as (
        select
            territory_id
            , sum(total_sales) as total_sales_2013
        from {{ ref('fct_salesdetails') }}
        where extract(year from order_date) = 2013
        group by territory_id
    )
    , sales_2014 as (
        select
            territory_id
            , sum(total_sales) as total_sales_2014
        from {{ ref('fct_salesdetails') }}
        where extract(year from order_date) = 2014
        group by territory_id
    )
    , agg_salesterritory as (
        select
            salesterritory.region as territory
            , sales_2013.total_sales_2013
            , sales_2014.total_sales_2014
            , fct_sales.total_sales
            , fct_sales.orders_qty
            , fct_sales.avg_ticket
            , fct_sales.first_order
            , fct_sales.last_order
        from salesterritory
        left join fct_sales on salesterritory.territory_id = fct_sales.territory_id
        left join sales_2014 on salesterritory.territory_id = sales_2014.territory_id
        left join sales_2013 on salesterritory.territory_id = sales_2013.territory_id    
    )
select *
from agg_salesterritory