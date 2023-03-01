{create_or_insert}
    payouts as (
        select 
            date_key,
            sum(post_monopoly_payout) as payout
        from {target_database}.kre_mart.daily_kin_payout
        where 
        -- We only want complete days
            date_key < dateadd('day', -40, date_trunc('day', current_date))
            {incremental_filter}
        group by date_key
        order by date_key asc
    ),
    user_balances as (
        select
            date_key,
            sum(closing_balance) as balance
        from {target_database}.kre_mart.closing_account_balance_per_day
        where 
        -- We only want complete days
            date_key < dateadd('day', -40, date_trunc('day', current_date))
            {incremental_filter}
        group by date_key
        order by date_key asc
    ),
    churn as (
        select 
            date_key,
            sum(balance) as amount
        from {target_database}.metabase_mart.daily_churn_volume
        where 
        -- We only want complete days
            date_key < dateadd('day', -40, date_trunc('day', current_date))
            {incremental_filter}
        group by date_key
        order by date_key asc
    )

    select
        p.date_key as date_key,
        -- By default, every day has a payment so our payout will never be null
        p.payout as daily_payout,
        ifnull(ub.balance, 0) as current_balance,
        ifnull(prev.balance, 0) as prev_balance,
        ifnull(c.amount, 0) as churn_amount,
        (daily_payout - (current_balance - prev_balance) - churn_amount) as net_inflation,
        case
            when net_inflation > 0 then 'Inflation'
            when net_inflation < 0 then 'Contraction'
            else 'None'
        end as inflation_flag
    from payouts p
    left join user_balances ub on p.date_key = ub.date_key
    left join user_balances prev on dateadd('day', -1, p.date_key) = prev.date_key
    left join churn c on p.date_key = c.date_key
    order by date_key desc
{query_close}
