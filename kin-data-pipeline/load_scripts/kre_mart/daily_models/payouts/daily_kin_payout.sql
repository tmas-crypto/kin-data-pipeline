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
    total_aub as (
        select 
            dd.date_key,
            sum(coalesce(aub.capped_aub, 0)) as capped_aub
        from distinct_dates dd
        join {target_database}.kre_mart.daily_active_user_balance_by_app aub on dd.date_key = aub.date_key
        group by dd.date_key
    ),
    pre_monopoly_payouts as (
        select
            dd.date_key,
            aub.app_id,
            (aub.capped_aub::NUMBER(38,10) / total.capped_aub) as app_aub_share,
            app_aub_share * 250000000 as pre_vf_payout,
            --Note: total_kre_payout hardcoded as 250000000. Should reduce over time. Should be analytics driven
            app_aub_share * (250000000 * (1 - vf.volatility_factor)) as payout,
            dense_rank() over (partition by dd.date_key order by payout desc) as daily_app_payout_rank
        from distinct_dates dd
        join {target_database}.kre_mart.daily_active_user_balance_by_app aub on dd.date_key = aub.date_key
        join total_aub total on aub.date_key = total.date_key
        join {target_database}.kre_mart.daily_volatility_factor vf on dd.date_key = vf.date_key
    ),
    post_monopoly_share_adjustments as (
        select
            -- We only want one row per date
            distinct pre.date_key,
            -- Getting unadjusted shares for calculations
            top_share.app_aub_share as top_share,
            second_top_share.app_aub_share as second_top_share,
            case
                -- If s_1 > 0.5 then s_1' = 0.5 + ((s_1 - 0.5) / 0.5) * (2/3 - 1/2)
                -- s_1 == top_share.app_aub_share
                -- s_1' == top_share_adjusted_initial
                when top_share.app_aub_share > 0.5
                then 0.5 + (((top_share.app_aub_share - 0.5) / 0.5) * ((2/3) - (1/2)))
                else top_share.app_aub_share
            end as top_share_adjusted_initial,
            case
                -- If s_1' + s_2  > 0.90 then s_2' = s_2 / (s_1+s_2) * 0.9
                -- s_2 == second_top_share.app_aub_share
                -- s_2' == second_top_share_adjusted
                when top_share_adjusted_initial + second_top_share.app_aub_share > 0.9
                then (second_top_share.app_aub_share / (top_share.app_aub_share + second_top_share.app_aub_share)) * 0.9
                else second_top_share.app_aub_share
            end as second_top_share_adjusted,
            -- s_1’ = minimum(s_1' / (s_1'+s_2) * 0.9, s_1')
            -- s_1' == top_share_adjusted
            case
                when top_share_adjusted_initial < (top_share_adjusted_initial / (top_share_adjusted_initial + second_top_share.app_aub_share)) * 0.9
                then top_share_adjusted_initial
                else (top_share_adjusted_initial / (top_share_adjusted_initial + second_top_share.app_aub_share)) * 0.9
            end as top_share_adjusted
        from pre_monopoly_payouts pre
        join pre_monopoly_payouts top_share on pre.date_key = top_share.date_key
            and top_share.daily_app_payout_rank = 1
        join pre_monopoly_payouts second_top_share on top_share.date_key = second_top_share.date_key
            and second_top_share.daily_app_payout_rank = 2
    ),
    daily_payout_share_sums as (
        select 
            pre_monopoly.date_key,
            sum(pre_monopoly.payout) as pre_monopoly_payout_sum,
            -- No monopoly. This should equal 1
            sum(pre_monopoly.app_aub_share) as pre_monopoly_sum,
            -- When s_2' = s_2 then sum (s_2...s_n)
            -- 1 - top_share
            pre_monopoly_sum - max(top_shares.top_share) as single_monopoly_sum,
            -- When s_2' != s_2 then sum (s_3...s_n)
            -- 1 - top_share - second_top_share
            pre_monopoly_sum - (max(top_shares.top_share) + max(top_shares.second_top_share)) as double_monopoly_sum
        from pre_monopoly_payouts pre_monopoly
        join post_monopoly_share_adjustments top_shares on pre_monopoly.date_key = top_shares.date_key
        group by pre_monopoly.date_key
    ),
    pre_second_share as (
        select
            date_key,
            app_aub_share
        from pre_monopoly_payouts
        where daily_app_payout_rank = 2
    ),
    daily_payout_staging as (
        select 
            pre.date_key,
            pre.app_id,
            case
                -- If (s_1 + s_2  > 0.90), s_2’ != s_2
                when pre_second_share.app_aub_share != post_adjustments.second_top_share_adjusted and pre.daily_app_payout_rank > 2
                then (pre.app_aub_share / payout_totals.double_monopoly_sum) * 0.1
                --If (s_1 > 0.5), s_2 == s_2'
                -- This condition is if the second highet share is not adjusted, ie. s_1 = > 0.5
                when pre_second_share.app_aub_share = post_adjustments.second_top_share_adjusted and post_adjustments.top_share_adjusted > 0.5 and pre.daily_app_payout_rank > 1
                then (pre.app_aub_share / payout_totals.single_monopoly_sum) * (1 - post_adjustments.top_share_adjusted)
                -- Check if top share has been adjusted
                when pre.daily_app_payout_rank = 1 and pre.app_aub_share != post_adjustments.top_share_adjusted
                then post_adjustments.top_share_adjusted
                -- No monopoly
                else pre.app_aub_share
            end as post_monopoly_app_share,
            (payout_totals.pre_monopoly_payout_sum * post_monopoly_app_share) as payout
        from pre_monopoly_payouts pre
        join pre_second_share pre_second_share on pre.date_key = pre_second_share.date_key
        join post_monopoly_share_adjustments post_adjustments on pre.date_key = post_adjustments.date_key
        join daily_payout_share_sums payout_totals on pre.date_key = payout_totals.date_key
    )
    select 
        pre.date_key,
        pre.app_id,
        dka.name as app_name,
        pre.app_aub_share as pre_monopoly_share,
        pre.pre_vf_payout,
        pre.payout as pre_monopoly_payout,
        post.post_monopoly_app_share,
        post.payout as post_monopoly_payout,
        (post.payout * vf.average_price) as post_monopoly_payout_usd
    from pre_monopoly_payouts pre
    join daily_payout_staging post on post.date_key = pre.date_key 
        and post.app_id = pre.app_id
    join {target_database}.kre_mart.daily_volatility_factor vf on pre.date_key = vf.date_key
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on pre.app_id = dka.id
    order by date_key asc
{query_close}
