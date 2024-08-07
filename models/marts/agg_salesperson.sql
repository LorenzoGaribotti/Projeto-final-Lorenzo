with
    person as (
        select *
        from {{ ref('stg_sap__person') }}
    )
    , salesperson as (
        select *
        from {{ ref('stg_sap__salesperson') }}
    )
    , agg_salesperson as (
        select
            salesperson.person_id
            , person.full_name
            , salesperson.salesquota
            , salesperson.bonus
            , salesperson.commissionpct
            , salesperson.salesytd
            , salesperson.saleslastyear
        from salesperson
        left join person on salesperson.person_id = person.businessentity_id
    )
select *
from agg_salesperson