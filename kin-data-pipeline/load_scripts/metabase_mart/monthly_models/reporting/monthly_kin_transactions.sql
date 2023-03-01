{create_or_insert}
    monthly_kin_txns as (
        select
            date_trunc('month', date_key) as date_key,
            app_id,
            sum(daily_total_transactions) as monthly_count,
            sum(daily_total_amounts) as monthly_amount
        from {target_database}.metabase_mart.daily_kin_transactions -- TODO: Update this
        -- We only want complete months loaded
        where date_key < date_trunc('month', current_date)
        {incremental_filter}
        group by
            date_trunc('month', date_key), app_id
    )
    select mkt.*, dka.name as app_name 
    from monthly_kin_txns mkt
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on mkt.app_id = dka.id
{query_close}
