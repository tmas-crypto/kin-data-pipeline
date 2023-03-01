{create_or_insert}
    daily_creates_by_app_and_type as (
        select 
            date_key,
            app_id,
            type as create_type,
            count(id) as daily_transaction_count,
            sum(balance) as daily_opening_balance
        from {target_database}.kin_main_warehouse.fact_kin_creations
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, type 
    )
    select 
        types.date_key,
        types.app_id,
        types.create_type as transaction_type,
        types.daily_transaction_count as daily_transactions,
        types.daily_opening_balance as daily_opening_balance,
        dka.name as app_name
    from daily_creates_by_app_and_type types
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on types.app_id = dka.id
    order by date_key asc
{query_close}
