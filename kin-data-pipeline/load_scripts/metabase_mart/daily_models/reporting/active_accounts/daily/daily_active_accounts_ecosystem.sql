{create_or_insert}
    active_spenders_raw as (
        select
            date_key,
            from_account as account
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        -- Peer txn from_account = spender
        and type in (2, 3)
    ),
    spenders_agg as (
        select
            date_key,
            count(distinct account) as account_count
        from active_spenders_raw
        group by date_key
    ),
    active_earners_raw as (
        select
            date_key,
            to_account as account
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        -- Peer txn to_account = earner
        and type in (1, 3)
    ),
    earners_agg as (
        select
            date_key,
            count(distinct account) as account_count
        from active_earners_raw
        group by date_key
    ),
    active_users_raw as (
        select * from active_spenders_raw
        union
        select * from active_earners_raw
    ),
    users_agg as (
        select
            date_key,
            count(distinct account) as account_count
        from active_users_raw
        group by date_key
    )
    select
        user.date_key,
        coalesce(user.account_count, 0) as active_users,
        coalesce(spend.account_count, 0) as active_spenders,
        coalesce(earn.account_count, 0) as active_earners
    from users_agg user
    left join spenders_agg spend on user.date_key = spend.date_key
    left join earners_agg earn on user.date_key = earn.date_key
{query_close}
