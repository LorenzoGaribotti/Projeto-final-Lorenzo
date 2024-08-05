with
    businessentityaddress as (
        select *
        from {{ ref('stg_sap__businessentityaddress') }}
    )
    , address as (
        select *
        from {{ ref('stg_sap__address') }}
    )
    , stateprovince as (
        select *
        from {{ ref('stg_sap__stateprovince') }}
    )
    , countryregion as (
        select *
        from {{ ref('stg_sap__countryregion') }}
    ) 
    , dim_location as (
        select
            {{ numeric_surrogate_key([
                'address.address_id'
            ]) }} as location_sk
            , address.address_id
            , businessentityaddress.businessentity_id
            , countryregion.country_code
            , stateprovince.stateprovince_id
            , stateprovince.state_code
            , stateprovince.statename
            , address.city
            , countryregion.country_name
        from address
        left join businessentityaddress on address.address_id = businessentityaddress.address_id
        left join stateprovince on address.stateprovince_id = stateprovince.stateprovince_id
        left join countryregion on stateprovince.country_code = countryregion.country_code
    )
select *
from dim_location