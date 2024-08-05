with
    source_stateprovince as (
        select
            cast(stateprovinceid as int) as stateprovince_id,
            cast(territoryid as int) as territory_id,
            stateprovince.stateprovincecode as state_code,
            stateprovince.countryregioncode as country_code,
            stateprovince.name as statename
        from {{ source('source', 'stateprovince') }}
    )
select *
from source_stateprovince
