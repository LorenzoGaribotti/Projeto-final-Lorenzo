with
    source_store as (
        select
            cast(businessentityid as int) as businessentity_id   
            , cast(salespersonid as int) as salesperson_id      
            , name as store_name
        from {{ source('source', 'store') }}
    )
select *
from source_store
