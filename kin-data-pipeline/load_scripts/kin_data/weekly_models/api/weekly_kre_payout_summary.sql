{create_or_insert}
    weekly_payouts_by_app_ranked as (
        select
            date_key as pg_date_key,
            app_id,
            app_name,
            weekly_kin_payout,
            weekly_usd_payout,
            dense_rank() over (partition by date_key order by weekly_kin_payout desc) as payout_rank
        from {target_database}.metabase_mart.kre_weekly_payout
        where True
        {incremental_filter}
    ),
    payout_total as (
        select
            pg_date_key,
            sum(weekly_kin_payout) as total_kin_payout,
            sum(weekly_usd_payout) as total_usd_payout
        from weekly_payouts_by_app_ranked
        group by pg_date_key
    ),
    top_10 as (
        select
            pg_date_key,
            array_agg(object_construct(
                'id', app_id,
                'name', app_name,
                'kinPayout', weekly_kin_payout,
                'usdPayout', weekly_usd_payout
            )) as data_object
        from weekly_payouts_by_app_ranked
        where payout_rank <= 10
        group by pg_date_key
    )
    select 
        payout_total.pg_date_key as date_key,
        payout_total.total_kin_payout as kin_payout,
        payout_total.total_usd_payout as usd_payout,
        to_json(top_10.data_object) as top_app_list
    from payout_total
    join top_10 on payout_total.pg_date_key = top_10.pg_date_key
    order by payout_total.pg_date_key desc
{query_close}
