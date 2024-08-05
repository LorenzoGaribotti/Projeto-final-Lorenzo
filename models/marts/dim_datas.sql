with
    array_de_datas as (
        {{
            dbt_utils.date_spine(
                datepart="day"
                , start_date="cast('2011-01-01' as date)"
                , end_date="cast('2016-01-01' as date)"
            )
        }}
    )

    , ajuste_de_casting as (
        select cast(date_day as date) as data_dia
        from array_de_datas
    )

    , datas as (
        select
            data_dia as data_metrica
            , date_trunc(data_dia, week (monday)) as semana_referencia
            , date_trunc(data_dia, month) as mes_referencia
            , unix_seconds(timestamp(data_dia)) as epoch
            , format_date('%d', data_dia) as sufixo_dia
            , case
                when format_date('%A', data_dia) = 'Sunday' then 'Domingo'
                when format_date('%A', data_dia) = 'Monday' then 'Segunda'
                when format_date('%A', data_dia) = 'Tuesday' then 'Terça'
                when format_date('%A', data_dia) = 'Wednesday' then 'Quarta'
                when format_date('%A', data_dia) = 'Thursday' then 'Quinta'
                when format_date('%A', data_dia) = 'Friday' then 'Sexta'
                when format_date('%A', data_dia) = 'Saturday' then 'Sábado'
            end as nome_dia
            , extract(dayofweek from data_dia) as dia_da_semana
            , extract(day from data_dia) as dia_do_mes
            , date_diff(data_dia, date_trunc(data_dia, quarter), day) + 1 as dia_do_trimestre
            , extract(dayofyear from data_dia) as dia_do_ano
            , cast(format_date('%U', data_dia) as int64) as semana_do_mes
            , extract(week from data_dia) as semana_do_ano
            , format_date('%G-S%V-', data_dia)
            || cast(
                case
                    when extract(dayofweek from data_dia) = 1 then 7
                    else extract(dayofweek from data_dia) - 1
                end as string
            ) as semana_do_ano_iso
            , extract(month from data_dia) as mes_real
            , case
                when extract(month from data_dia) = 1 then 'Janeiro'
                when extract(month from data_dia) = 2 then 'Fevereiro'
                when extract(month from data_dia) = 3 then 'Março'
                when extract(month from data_dia) = 4 then 'Abril'
                when extract(month from data_dia) = 5 then 'Maio'
                when extract(month from data_dia) = 6 then 'Junho'
                when extract(month from data_dia) = 7 then 'Julho'
                when extract(month from data_dia) = 8 then 'Agosto'
                when extract(month from data_dia) = 9 then 'Setembro'
                when extract(month from data_dia) = 10 then 'Outubro'
                when extract(month from data_dia) = 11 then 'Novembro'
                when extract(month from data_dia) = 12 then 'Dezembro'
            end as nome_mes
            , case
                when extract(month from data_dia) = 1 then 'Jan'
                when extract(month from data_dia) = 2 then 'Fev'
                when extract(month from data_dia) = 3 then 'Mar'
                when extract(month from data_dia) = 4 then 'Abr'
                when extract(month from data_dia) = 5 then 'Mai'
                when extract(month from data_dia) = 6 then 'Jun'
                when extract(month from data_dia) = 7 then 'Jul'
                when extract(month from data_dia) = 8 then 'Ago'
                when extract(month from data_dia) = 9 then 'Set'
                when extract(month from data_dia) = 10 then 'Out'
                when extract(month from data_dia) = 11 then 'Nov'
                when extract(month from data_dia) = 12 then 'Dez'
            end as nome_abreviado_mes
            , extract(quarter from data_dia) as trimestre_real
            , case
                when extract(quarter from data_dia) = 1 then 'primeiro'
                when extract(quarter from data_dia) = 2 then 'segundo'
                when extract(quarter from data_dia) = 3 then 'terceiro'
                when extract(quarter from data_dia) = 4 then 'quarto'
            end as nome_trimestre
            , extract(year from data_dia) as ano_real
            , date_add(data_dia, interval 1 - extract(dayofweek from data_dia) day) as primeiro_dia_da_semana
            , date_add(data_dia, interval 7 - extract(dayofweek from data_dia) day) as ultimo_dia_da_semana
            , date_trunc(data_dia, month) as primeiro_dia_do_mes
            , date_sub(date_add(date_trunc(data_dia, month), interval 1 month), interval 1 day) as ultimo_dia_do_mes
            , date_trunc(data_dia, quarter) as primeiro_dia_do_trimestre
            , date_sub(date_add(date_trunc(data_dia, quarter), interval 3 month), interval 1 day) as ultimo_dia_do_trimestre
            , date(format_date('%Y-01-01', data_dia)) as primeiro_dia_do_ano
            , date(format_date('%Y-12-31', data_dia)) as ultimo_dia_do_ano
            , format_date('%m%Y', data_dia) as mes_ano
            , format_date('%m%d%Y', data_dia) as mes_dia_ano
            , case
                when extract(dayofweek from data_dia) in (1, 7) then 'Final de semana'
                else 'Dia útil'
            end as flag_final_de_semana
        from ajuste_de_casting
        order by data_dia desc
    )

select *
from datas
