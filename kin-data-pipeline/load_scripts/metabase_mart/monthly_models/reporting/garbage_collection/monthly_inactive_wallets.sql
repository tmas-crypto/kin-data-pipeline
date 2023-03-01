{create_or_insert}
    -- Runs a month behind. Ie. run_date = 09-01 GCs for the month of July. Req at least one month of inactivity
    distinct_wallets_with_last_zero_balance as (
        select
            date_key,
            wallet_id
        from {target_database}.metabase_mart.closing_account_balance_per_day
        where closing_balance <= 0
        -- We want records prior to the run date in the current month
        and date_key < dateadd('month', -1, date_trunc('month', current_date))
        -- We only want records from the start of the last month to the end as previous wallets would have be GC'd
        and date_key >= dateadd('month', -2, date_trunc('month', current_date))
        {incremental_filter}
    ),
    last_date_zero_balance as (
        select wallet_id, max(date_key) as max_date
        from distinct_wallets_with_last_zero_balance
        group by wallet_id
    ),
    non_zero_balances_after_last_zero_balance as (
        select 
            distinct zero_bal.wallet_id
        from last_date_zero_balance zero_bal
        join {target_database}.metabase_mart.closing_account_balance_per_day close on zero_bal.wallet_id = close.wallet_id
        where zero_bal.max_date < close.date_key and close.closing_balance > 0
    )
    select
        dateadd('month', 1, date_trunc('month', zb.max_date)) as date_key,
        zb.wallet_id
    from last_date_zero_balance zb
    where zb.wallet_id not in (select wallet_id from non_zero_balances_after_last_zero_balance)
{query_close}
