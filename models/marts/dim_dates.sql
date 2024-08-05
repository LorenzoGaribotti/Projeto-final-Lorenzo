with
    date_array as (
        {{
            dbt_utils.date_spine(
                datepart="day"
                , start_date="cast('2011-01-01' as date)"
                , end_date="cast('2016-01-01' as date)"
            )
        }}
    )

    , casting_adjustment as (
        select cast(date_day as date) as date_day
        from date_array
    )

    , dates as (
        select
            date_day as date_metric
            , date_trunc(date_day, week (monday)) as reference_week
            , date_trunc(date_day, month) as reference_month
            , unix_seconds(timestamp(date_day)) as epoch
            , format_date('%d', date_day) as day_suffix
            , case
                when format_date('%A', date_day) = 'Sunday' then 'Sunday'
                when format_date('%A', date_day) = 'Monday' then 'Monday'
                when format_date('%A', date_day) = 'Tuesday' then 'Tuesday'
                when format_date('%A', date_day) = 'Wednesday' then 'Wednesday'
                when format_date('%A', date_day) = 'Thursday' then 'Thursday'
                when format_date('%A', date_day) = 'Friday' then 'Friday'
                when format_date('%A', date_day) = 'Saturday' then 'Saturday'
            end as day_name
            , extract(dayofweek from date_day) as day_of_week
            , extract(day from date_day) as day_of_month
            , date_diff(date_day, date_trunc(date_day, quarter), day) + 1 as day_of_quarter
            , extract(dayofyear from date_day) as day_of_year
            , cast(format_date('%U', date_day) as int64) as week_of_month
            , extract(week from date_day) as week_of_year
            , format_date('%G-S%V-', date_day)
            || cast(
                case
                    when extract(dayofweek from date_day) = 1 then 7
                    else extract(dayofweek from date_day) - 1
                end as string
            ) as week_of_year_iso
            , extract(month from date_day) as real_month
            , case
                when extract(month from date_day) = 1 then 'January'
                when extract(month from date_day) = 2 then 'February'
                when extract(month from date_day) = 3 then 'March'
                when extract(month from date_day) = 4 then 'April'
                when extract(month from date_day) = 5 then 'May'
                when extract(month from date_day) = 6 then 'June'
                when extract(month from date_day) = 7 then 'July'
                when extract(month from date_day) = 8 then 'August'
                when extract(month from date_day) = 9 then 'September'
                when extract(month from date_day) = 10 then 'October'
                when extract(month from date_day) = 11 then 'November'
                when extract(month from date_day) = 12 then 'December'
            end as month_name
            , case
                when extract(month from date_day) = 1 then 'Jan'
                when extract(month from date_day) = 2 then 'Feb'
                when extract(month from date_day) = 3 then 'Mar'
                when extract(month from date_day) = 4 then 'Apr'
                when extract(month from date_day) = 5 then 'May'
                when extract(month from date_day) = 6 then 'Jun'
                when extract(month from date_day) = 7 then 'Jul'
                when extract(month from date_day) = 8 then 'Aug'
                when extract(month from date_day) = 9 then 'Sep'
                when extract(month from date_day) = 10 then 'Oct'
                when extract(month from date_day) = 11 then 'Nov'
                when extract(month from date_day) = 12 then 'Dec'
            end as abbreviated_month_name
            , extract(quarter from date_day) as real_quarter
            , case
                when extract(quarter from date_day) = 1 then 'first'
                when extract(quarter from date_day) = 2 then 'second'
                when extract(quarter from date_day) = 3 then 'third'
                when extract(quarter from date_day) = 4 then 'fourth'
            end as quarter_name
            , extract(year from date_day) as real_year
            , date_add(date_day, interval 1 - extract(dayofweek from date_day) day) as first_day_of_week
            , date_add(date_day, interval 7 - extract(dayofweek from date_day) day) as last_day_of_week
            , date_trunc(date_day, month) as first_day_of_month
            , date_sub(date_add(date_trunc(date_day, month), interval 1 month), interval 1 day) as last_day_of_month
            , date_trunc(date_day, quarter) as first_day_of_quarter
            , date_sub(date_add(date_trunc(date_day, quarter), interval 3 month), interval 1 day) as last_day_of_quarter
            , date(format_date('%Y-01-01', date_day)) as first_day_of_year
            , date(format_date('%Y-12-31', date_day)) as last_day_of_year
            , format_date('%m%Y', date_day) as month_year
            , format_date('%m%d%Y', date_day) as month_day_year
            , case
                when extract(dayofweek from date_day) in (1, 7) then 'Weekend'
                else 'Weekday'
            end as weekend_flag
        from casting_adjustment
        order by date_day desc
    )

select *
from dates
