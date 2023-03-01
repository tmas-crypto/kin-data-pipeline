{create_or_insert}
    daily_transactions as (
        select
            date_key as pg_date_key,
            sum(daily_total_transactions) as total_daily_transactions,
            count(distinct app_id)::NUMBER(18,0) as daily_active_apps
        from {target_database}.metabase_mart.daily_kin_transactions
        where True
        {incremental_filter}
        group by date_key
    ),
    aub as (
        select
            date_key as pg_date_key,
            sum(aub) as total_aub,
            sum(capped_aub) as total_capped_aub,
            sum(au) as total_au
        from {target_database}.metabase_mart.daily_active_user_balance_by_app
        where True
        {incremental_filter}
        group by date_key
    ),
    monthly_active_earners as (
        select
            date_key as pg_date_key,
            active_earners as wallet_count
        from {target_database}.metabase_mart.monthly_active_accounts_ecosystem
        where True
        {incremental_filter}
    ),
    monthly_active_spenders as (
        select
            date_key as pg_date_key,
            active_spenders as wallet_count
        from {target_database}.metabase_mart.monthly_active_accounts_ecosystem
        where True
        {incremental_filter}
    ),
    monthly_active_users as (
        select
            date_key as pg_date_key,
            active_users as wallet_count
        from {target_database}.metabase_mart.monthly_active_accounts_ecosystem
        where True
        {incremental_filter}
    ),
    volatility_factor as (
        select
            date_key as pg_date_key,
            volatility_factor
        from {target_database}.metabase_mart.daily_volatility_factor
        where True
        {incremental_filter}
    ),
    payouts as (
        select
            date_key as pg_date_key,
            post_monopoly_payout as daily_payout_kin,
            post_monopoly_payout_usd as daily_payout_usd
        from {target_database}.metabase_mart.daily_kin_payout
        where True
        {incremental_filter}
    )
    select
        dt.pg_date_key as date_key,
        dt.daily_active_apps as active_apps,
        aub.total_aub as active_user_balance,
        aub.total_capped_aub as active_capped_user_balance,
        aub.total_au as active_users,
        vf.volatility_factor as volatility_factor,
        p.daily_payout_kin as kin_payout,
        p.daily_payout_usd as usd_payout,
        dt.total_daily_transactions as transaction_count,
        mae.wallet_count as monthly_active_earners,
        mas.wallet_count as monthly_active_spenders,
        mau.wallet_count as monthly_active_users
    from daily_transactions dt
    join aub aub on dt.pg_date_key = aub.pg_date_key
    join monthly_active_earners mae on dt.pg_date_key = mae.pg_date_key
    join monthly_active_spenders mas on dt.pg_date_key = mas.pg_date_key
    join monthly_active_users mau on dt.pg_date_key = mau.pg_date_key
    join volatility_factor vf on dt.pg_date_key = vf.pg_date_key
    join payouts p on dt.pg_date_key = p.pg_date_key
    order by dt.pg_date_key asc
{query_close}
