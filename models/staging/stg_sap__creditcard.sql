with
    source_creditcard as (
        select
            cast(creditcardid as int) as creditcard_id        
            , creditcard.cardtype as cardtype
        from {{ source('source', 'creditcard') }}
    )
select *
from source_creditcard
