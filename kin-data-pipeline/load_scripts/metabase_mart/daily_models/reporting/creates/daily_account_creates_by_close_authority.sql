{create_or_insert}
    daily_creates_by_app_and_authority as (
        select 
            date_key,
            app_id,
            close_account_authority as close_authority,
            count(id) as daily_transaction_count,
            sum(balance) as daily_opening_balance
        from {target_database}.kin_main_warehouse.fact_kin_creations
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, close_account_authority 
    )
    select 
        close_authority.date_key,
        close_authority.app_id,
        close_authority.close_authority,
        close_authority.daily_transaction_count as daily_transactions,
        close_authority.daily_opening_balance as daily_opening_balance,
        dka.name as app_name
    from daily_creates_by_app_and_authority close_authority
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on close_authority.app_id = dka.id
    order by date_key asc
{query_close}
