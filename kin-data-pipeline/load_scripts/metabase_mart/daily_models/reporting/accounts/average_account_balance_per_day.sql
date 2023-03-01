{create_or_insert}
    from_account_balances as (
        select
            date_key,
            from_account,
            avg(from_account_balance) as from_balance_average
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, from_account
    ),
    to_account_balances as (
        select
            date_key,
            to_account,
            avg(to_account_balance) as to_balance_average
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, to_account
    ),
    balance_union as (
        select 
            date_key, 
            from_account as wallet_id, 
            from_balance_average as balance_average 
        from from_account_balances
        union all
        select 
            date_key, 
            to_account as wallet_id, 
            to_balance_average as balance_average
        from to_account_balances
    )
    select 
        date_key,
        wallet_id,
        avg(balance_average) as account_balance_average
    from balance_union
    group by date_key, wallet_id
    order by date_key asc
{query_close}
