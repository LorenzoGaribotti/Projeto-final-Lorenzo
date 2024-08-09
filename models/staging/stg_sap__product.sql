with
    source_product as (
        select
            cast(productid as int) as product_id
            , cast(productsubcategoryid as int) as productsubcategory_id
            , product.name as productname
            , product.color as productcolor
            , product.standardcost as cost
            , product.listprice as listprice
        from {{ source('source', 'product') }}
    )
select *
from source_product
