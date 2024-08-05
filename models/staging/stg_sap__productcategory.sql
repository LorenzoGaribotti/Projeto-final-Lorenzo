with
    source_productcategory as (
        select
            cast(productcategoryid as int) as productcategory_id,
            productcategory.name as productcategoryname
        from {{ source('source', 'productcategory') }}
    )
select *
from source_productcategory
