with
    source_businessentityaddress as (
        select
            cast(addressid as int) as address_id           
            , cast(businessentityid as int) as businessentity_id
        from {{ source('source', 'businessentityaddress') }}
    )
select *
from source_businessentityaddress
