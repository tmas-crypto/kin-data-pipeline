{create_or_insert}
    daily_creates_by_app as (
        select 
            date_key,
            app_id,
            count(id) as daily_transaction_count,
            sum(balance) as daily_balance,
            sum(transaction_fee) as daily_transaction_fee_sum,
            sum(creation_fee) as daily_creation_fee_sum
        from {target_database}.kin_main_warehouse.fact_kin_creations
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id
    )
    select 
        creates.date_key,
        creates.app_id,
        creates.daily_transaction_count as daily_transactions,
        creates.daily_balance as daily_opening_balance,
        creates.daily_transaction_fee_sum,
        creates.daily_creation_fee_sum,
        dka.name as app_name
    from daily_creates_by_app creates
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on creates.app_id = dka.id
    order by date_key asc
{query_close}
