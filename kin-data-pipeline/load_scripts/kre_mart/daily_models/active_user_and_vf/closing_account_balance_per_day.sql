{create_or_insert}
    created_accounts as (
        select
            date_key,
            account_id,
            max(to_timestamp(timestamp)) as closing_timestamp
        from {target_database}.kin_main_warehouse.fact_kin_creations
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, account_id
    ),
    from_account_last_timestamp as (
        select
            date_key,
            from_account,
            max(to_timestamp(timestamp)) as closing_timestamp
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, from_account
    ),
    to_account_last_timestamp as (
        select
            date_key,
            to_account,
            max(to_timestamp(timestamp)) as closing_timestamp
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, to_account
    ),
    timestamp_union as (
        -- created accounts
        select
            date_key,
            account_id as wallet_id,
            closing_timestamp
        from created_accounts
        union
        -- accounts sending kin
        select 
            date_key, 
            from_account as wallet_id, 
            closing_timestamp
        from from_account_last_timestamp
        union
        -- accounts receiving kin
        select 
            date_key, 
            to_account as wallet_id, 
            closing_timestamp
        from to_account_last_timestamp
    ),
    true_closing_timestamp as (
        select 
            date_key,
            wallet_id,
            -- Take the greater of the two timestamps
            max(closing_timestamp) as closing_timestamp
        from timestamp_union
        group by date_key, wallet_id
    ),
    account_balances_by_day as (
        select 
            ts.date_key,
            case
                when ts.wallet_id = kt.from_account then kt.from_account
                when ts.wallet_id = kt.to_account then kt.to_account
                when ts.wallet_id = kc.account_id then kc.account_id
                else 'Unknown'
            end as wallet_id,
            case
                when ts.wallet_id = kt.from_account then coalesce(kt.from_account_balance, 0)
                when ts.wallet_id = kt.to_account then coalesce(kt.to_account_balance, 0)
                when ts.wallet_id = kc.account_id then coalesce(kc.balance, 0)
                else 0
            end as closing_balance
        from true_closing_timestamp ts
        left join {target_database}.kin_main_warehouse.fact_kin_transaction kt on ts.date_key = kt.date_key
            and ts.closing_timestamp = to_timestamp(kt.timestamp)
            and (ts.wallet_id = kt.from_account or ts.wallet_id = kt.to_account)
        left join {target_database}.kin_main_warehouse.fact_kin_creations kc on ts.date_key = kc.date_key
            and ts.closing_timestamp = to_timestamp(kc.timestamp)
            and ts.wallet_id = kc.account_id
    )
    select 
        date_key, wallet_id, max(closing_balance)::NUMERIC as closing_balance
    from account_balances_by_day
    group by date_key, wallet_id
{query_close}
