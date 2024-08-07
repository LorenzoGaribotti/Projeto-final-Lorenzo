with
    dim_location as (
        select *
        from {{ ref('dim_location') }}
    )
    , fct_sales as (
        select
            fct_salesdetails.*
            , dim_location.city
            , dim_location.statename
            , dim_location.country_name
        from {{ ref('fct_salesdetails') }} as fct_salesdetails
        left join dim_location on fct_salesdetails.shiptoaddress_id = dim_location.address_id
    )
    , fct_metrics as (
        select
            city
            , count(distinct order_id) as orders_qt
            , avg(total_sales) as avg_ticket
            , sum(total_sales) as total_sales
            , min(order_date) as first_order
            , max(order_date) as last_order
        from fct_sales
        group by city
    )
    , agg_salesterritory as (
        select
            dim_location.city
            , dim_location.statename
            , dim_location.country_name
            , fct_metrics.total_sales
            , fct_metrics.orders_qt
            , fct_metrics.avg_ticket
            , fct_metrics.first_order
            , fct_metrics.last_order
        from dim_location
        left join fct_metrics on dim_location.city = fct_metrics.city
        group by
            dim_location.city
            , dim_location.statename
            , dim_location.country_name
            , fct_metrics.total_sales
            , fct_metrics.orders_qt
            , fct_metrics.avg_ticket
            , fct_metrics.first_order
            , fct_metrics.last_order
    )
    , dedup as (
        select
            *
            , row_number() over (
                partition by
                    city
                order by country_name desc
            ) as dedup_table    
        from agg_salesterritory   
    )
select *
from dedup
where dedup_table = 1