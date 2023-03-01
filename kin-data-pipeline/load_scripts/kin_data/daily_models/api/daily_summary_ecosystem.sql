{create_or_insert}
    daily_transactions as (
        select
            date_key as pg_date_key,
            count(distinct app_id) as app_count,
            sum(daily_fee_sum) as daily_fee_sum,
            sum(daily_total_transactions) as total_transactions,
            sum(daily_total_amounts) as total_amounts,
            sum(daily_earn_transactions) as earn_transactions,
            sum(daily_earn_amounts) as earn_amounts,
            sum(daily_spend_transactions) as spend_transactions,
            sum(daily_spend_amounts) as spend_amounts,
            sum(daily_peer_transactions) as peer_transactions,
            sum(daily_peer_amounts) as peer_amounts
        from {target_database}.metabase_mart.daily_kin_transactions
        where True
        {incremental_filter}
        group by date_key
    ),
    daily_users as (
        select
            users.date_key as pg_date_key,
            users.active_users as active_users,
            coalesce(users.active_earners, 0) as active_earners,
            coalesce(users.active_spenders, 0) as active_spenders
        from {target_database}.metabase_mart.daily_active_accounts_ecosystem users
        where True
        {incremental_filter}  
    ),
    monthly_users as (
        select
            users.date_key as pg_date_key,
            users.active_users as active_users,
            coalesce(users.active_earners, 0) as active_earners,
            coalesce(users.active_spenders, 0) as active_spenders
        from {target_database}.metabase_mart.monthly_active_accounts_ecosystem users
        where True
        {incremental_filter} 
    )
    select
        txns.pg_date_key as date_key,
        txns.app_count as app_count,
        txns.daily_fee_sum as transaction_fees_sol,
        (txns.daily_fee_sum * s.prices::NUMBER(38, 10)) as transactions_fee_usd,
        txns.total_transactions as transaction_count,
        txns.total_amounts as transaction_amount_kin,
        (txns.total_amounts*price.prices) as transaction_amount_usd,
        txns.earn_transactions as earn_count,
        txns.earn_amounts as earn_amount_kin,
        (txns.earn_amounts*price.prices) as earn_amount_usd,
        txns.spend_transactions as spend_count,
        txns.spend_amounts as spend_amount_kin,
        (txns.spend_amounts*price.prices) as spend_amount_usd,
        txns.peer_transactions as peer_count,
        txns.peer_amounts as peer_amount_kin,
        (txns.peer_amounts*price.prices) as peer_amount_usd,
        coalesce(du.active_users, 0) as daily_active_users,
        coalesce(du.active_earners, 0) as daily_active_earners,
        coalesce(du.active_spenders, 0) as daily_active_spenders,
        coalesce(mu.active_users, 0) as monthly_active_users,
        coalesce(mu.active_earners, 0) as monthly_active_earners,
        coalesce(mu.active_spenders, 0) as monthly_active_spenders
    from daily_transactions txns
    left join daily_users du on txns.pg_date_key = du.pg_date_key
    left join monthly_users mu on txns.pg_date_key = mu.pg_date_key
    join {target_database}.kin_main_warehouse.dim_kin_price price on txns.pg_date_key = price.date_key 
    join {target_database}.kin_main_warehouse.dim_solana_price s on txns.pg_date_key = s.date_key
    order by txns.pg_date_key asc
{query_close}
