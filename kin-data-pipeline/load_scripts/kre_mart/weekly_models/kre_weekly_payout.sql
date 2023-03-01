{create_or_insert}
    distinct_weeks as (
        select 
            distinct date_trunc('week', date_key) as date_key
        from 
            {target_database}.kre_mart.daily_kin_payout
        where
            date_key < date_trunc('week', current_date)
            {incremental_filter}
        group by
            date_trunc('week', date_key)
    ),
    weekly_kin_payouts as (
        select
            dw.date_key,
            dkp.app_id,
            sum(dkp.post_monopoly_payout) as weekly_kin_payout,
            sum(dkp.post_monopoly_payout_usd) as weekly_usd_payout
        from distinct_weeks dw
        -- Here we are interested in getting the sum of the previous week for the payout
        join {target_database}.kre_mart.daily_kin_payout dkp on 
            -- Previous week Sunday less than or equal to this past Sunday
            -- 07-17 < 07-24
            dateadd('week', -1, dateadd('day', -1, dw.date_key)) <= dkp.date_key
            and dateadd('day', -2, dw.date_key) >= dkp.date_key
        group by
            dw.date_key, app_id
    )
    select
        wkt.*,
        wkt.date_key as run_date,
        dateadd('week', -1, dateadd('day', -1, wkt.date_key)) as from_date,
        dateadd('day', -2, wkt.date_key) as to_date,
        dka.name as app_name,
        dka.public_wallet as wallet
    from weekly_kin_payouts wkt
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on wkt.app_id = dka.id
    order by date_key asc
{query_close}