{create_or_insert}
    distinct_dates as (
        select
            distinct date_key
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- Hard coding this for now based on the amount of dates we have to lookback on
        where
            -- We only want complete days
            date_key < dateadd('day', -40, date_trunc('day', current_date))
            {incremental_filter}
    ),
    transacted_accounts as (
        select
            dd.date_key as date_key,
            kt.app_id as app_id,
            bal.wallet_id as wallet_id
        from distinct_dates dd
        -- We do this join to associated an app_id to determine churn by app
        join {target_database}.kin_main_warehouse.fact_kin_transaction kt on dd.date_key = kt.date_key
        join {target_database}.kre_mart.closing_account_balance_per_day bal on dd.date_key = bal.date_key
            and kt.from_account = bal.wallet_id
        -- In this case, we're interested in apps that transact within ecosystem apps.
        where kt.app_id != 0
        group by dd.date_key, kt.app_id, bal.wallet_id
    ),
    accounts_with_balances as (
        select
            dd.date_key as date_key,
            ta.app_id as app_id,
            ta.wallet_id as wallet_id,
            bal.closing_balance as balance
        from distinct_dates dd
        join transacted_accounts ta on dd.date_key = ta.date_key
        join {target_database}.kre_mart.closing_account_balance_per_day bal on dd.date_key = bal.date_key
            and ta.wallet_id = bal.wallet_id
    ),
    daily_churn_summary as (
        select
            dd.date_key,
            awb.app_id,
            awb.wallet_id,
            awb.balance
        from distinct_dates dd
        join accounts_with_balances awb on dd.date_key = awb.date_key
        left join {target_database}.kre_mart.closing_account_balance_per_day bal on bal.date_key > dd.date_key
            and bal.date_key <= dateadd('day', 40, dd.date_key)
            and awb.wallet_id = bal.wallet_id
        where bal.wallet_id is null
    )
    select * from daily_churn_summary
    order by date_key asc
{query_close}
