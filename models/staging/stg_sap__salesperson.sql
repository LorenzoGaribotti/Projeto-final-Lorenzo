with
    source_salesperson as (
        select
            cast(businessentityid as int) as person_id
            , cast(salesquota as int) as salesquota
            , cast(bonus as int) as bonus
            , cast(commissionpct as numeric) as commissionpct
            , cast(salesytd as numeric) as salesytd
            , cast(saleslastyear as numeric) as saleslastyear
        from {{ source('source', 'salesperson') }}
    )
select *
from source_salesperson
