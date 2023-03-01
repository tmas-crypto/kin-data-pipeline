{create_or_insert}
    distinct_dates as (
        select 
            distinct date_key
        from 
            {target_database}.kin_main_warehouse.fact_kin_transaction
        where
            -- Hard coding this for now based on the amount of dates we have to lookback on
            date_key >= '2021-11-10' and
            -- We only want complete days
            date_key < date_trunc('day', current_date)
            {incremental_filter}
    ),
    -- This is important because it tells us if a user has had 3 spend transactions per date per app.
    kre_active_users as (
        select
            dd.date_key as date_key,
            case
                when kt.app_id is null then 0
                else kt.app_id
            end as app_id,
            from_account as wallet_id,
            count(kt.id) as spend_count,
            sum(amount) as total_spend
        from distinct_dates dd
        join {target_database}.kin_main_warehouse.fact_kin_transaction kt on kt.date_key >= dateadd('day', -29, dd.date_key)
            and kt.date_key <= dd.date_key
        where kt.type in (2, 3)
        group by dd.date_key, app_id, wallet_id
    ),
    au_filtered as (
        select kau.*
        from kre_active_users kau
        where kau.spend_count >= 3
        -- Ultimately I think our spend amount threshold will apply here too (ie. 1c of Kin)
        and app_id != 0
    ),
    closing_dates as (
        select 
            au.date_key,
            au.wallet_id,
            max(balances.date_key) as last_wallet_balance_date
        from au_filtered au
        left join {target_database}.kre_mart.closing_account_balance_per_day balances 
            -- Looking back the same time period to see if there's a balance
            -- By definition, this will always be true
            on balances.date_key >= dateadd('day', -29, au.date_key)
            and balances.date_key <= au.date_key
            and au.wallet_id = balances.wallet_id
        group by au.date_key, au.wallet_id
    ),
    wallet_balances as (
        select
            cd.date_key,
            cd.wallet_id,
            coalesce(bal.closing_balance, 0) as closing_balance
        from closing_dates cd
        join {target_database}.kre_mart.closing_account_balance_per_day bal on bal.date_key = cd.last_wallet_balance_date
            and bal.wallet_id = cd.wallet_id
    ),
    daily_app_summaries as (
        select 
            au.date_key,
            au.app_id,
            sum(wb.closing_balance)::NUMERIC as aub,
            count(au.wallet_id) as au,
            case
                when aub > au*100000 then au*100000::NUMERIC
                else aub
            end as capped_aub
        from wallet_balances wb
        join au_filtered au on wb.wallet_id = au.wallet_id
            and wb.date_key = au.date_key
        -- Take the most recent balance for the days lookback period
        group by au.date_key, au.app_id
    )
    select summaries.*, dka.name
    from daily_app_summaries summaries
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on summaries.app_id = dka.id
    order by summaries.date_key asc, summaries.app_id asc
{query_close}
