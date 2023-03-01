{create_or_insert}
    distinct_dates as (
        select 
            distinct date_key
        from 
            {target_database}.kin_main_warehouse.fact_kin_transaction
        where
            -- Hard coding this for now based on the amount of dates we have to lookback on
            date_key >= '2021-11-10' and
            -- We only want complete days
            date_key < date_trunc('day', current_date)
            {incremental_filter}
    ),
    daily_closing_prices as (
        select 
            date_key as date_key,
            prices as closing_price
        from {target_database}.kin_main_warehouse.dim_kin_price
    ),
    month_price_average as (
        select
            dd.date_key as date_key,
            avg(dcp.closing_price) as average_price
        from distinct_dates dd
        join daily_closing_prices dcp on dcp.date_key >= dateadd('day', -29, dd.date_key)
            and dcp.date_key <= dd.date_key
        group by dd.date_key
    ),
    daily_price_deviation as (
        select 
            dd.date_key,
            abs(dcp.closing_price - mpa.average_price) as price_deviation
        from distinct_dates dd
        join month_price_average mpa on dd.date_key = mpa.date_key
        join daily_closing_prices dcp on dcp.date_key >= dateadd('day', -29, dd.date_key)
            and dcp.date_key <= dd.date_key
    ),
    vf_summary as (
        select 
            dd.date_key,
            mpa.average_price,
            sum(dpd.price_deviation) as total_price_deviation,
            avg(dpd.price_deviation) as average_price_deviation,
            average_price_deviation/mpa.average_price as volatility_factor
        from distinct_dates dd
        join month_price_average mpa on dd.date_key = mpa.date_key
        join daily_price_deviation dpd on dd.date_key = dpd.date_key 
        group by dd.date_key, mpa.average_price
    )
    select vs.*
    from vf_summary vs
    order by vs.date_key asc
{query_close}
