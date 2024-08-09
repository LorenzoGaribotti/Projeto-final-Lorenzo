with
    source_salesterritory as (
        select
            cast(territoryid as int) as territory_id
            , name as region
            , countryregioncode as country_code
            , `group` as continent
            , cast(salesytd as numeric) as regionsalesyear
            , cast(saleslastyear as numeric) as regionsaleslastyear
        from {{ source('source', 'salesterritory') }}
    )
select *
from source_salesterritory
