with
    source_personcreditcard as (
        select
            cast(businessentityid as int) as businessentity_id,
            cast(creditcardid as int) as creditcard_id
        from {{ source('source', 'personcreditcard') }}
    )
select *
from source_personcreditcard
