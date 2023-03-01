{create_or_insert}
    weekly_kin_txns as (
        select
            date_trunc('week', date_key) as date_key,
            app_id,
            sum(daily_total_transactions) as weekly_count,
            sum(daily_total_amounts) as weekly_amount
        from {target_database}.metabase_mart.daily_kin_transactions
        -- We only want full weeks
        where date_key < date_trunc('week', current_date)
        {incremental_filter}
        group by
            date_trunc('week', date_key), app_id
    )
    select wkt.*, dka.name as app_name 
    from weekly_kin_txns wkt
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on wkt.app_id = dka.id
    order by date_key asc
{query_close}
