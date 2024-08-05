with
    source_productsubcategory as (
        select
            cast(productsubcategoryid as int) as productsubcategory_id,
            cast(productcategoryid as int) as productcategory_id,
            productsubcategory.name as productsubcategoryname
        from {{ source('source', 'productsubcategory') }}
    )
select *
from source_productsubcategory
