{create_or_insert}
    market_summary as (
        select
            date_key as pg_date_key,
            prices as daily_price,
            market_caps as daily_market_cap,
            total_volumes as daily_trade_volume,
            (market_caps/prices) as daily_circulating_supply
        from {target_database}.kin_main_warehouse.dim_kin_price
        where True
        {incremental_filter}
    )
    select
        pg_date_key as date_key,
        daily_price as kin_price,
        daily_market_cap as kin_market_cap,
        daily_trade_volume as kin_trade_volume,
        daily_circulating_supply as kin_circulating_supply
    from market_summary
{query_close}
