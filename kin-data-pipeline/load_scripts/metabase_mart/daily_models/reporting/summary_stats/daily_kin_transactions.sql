{create_or_insert}
    daily_transactions_by_app as (
        select 
            date_key,
            case
                when app_id is null then 0
                else app_id
            end as app_id,
            count(id) as daily_transaction_count,
            sum(amount) as daily_amount,
            -- Hardcoded until we swithc to fact table source
            sum(0.00001::NUMERIC(38, 10)) as daily_fee_sum
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id
    ),
    daily_earns_by_app as (
        select 
            date_key,
            case
                when app_id is null then 0
                else app_id
            end as app_id,
            count(id) as daily_transaction_count,
            sum(amount) as daily_amount,
            avg(amount) as average_amount,
            median(amount) as median_amount
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        and type = 1
        {incremental_filter}
        group by date_key, app_id  
    ),
    daily_spends_by_app as (
        select 
            date_key,
            case
                when app_id is null then 0
                else app_id
            end as app_id,
            count(id) as daily_transaction_count,
            sum(amount) as daily_amount,
            avg(amount) as average_amount,
            median(amount) as median_amount
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        and type = 2
        {incremental_filter}
        group by date_key, app_id
    ),
    daily_peer_by_app as (
        select
            date_key,
            case
                when app_id is null then 0
                else app_id
            end as app_id,
            count(id) as daily_transaction_count,
            sum(amount) as daily_amount,
            avg(amount) as average_amount,
            median(amount) as median_amount
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        and type = 3
        {incremental_filter}
        group by date_key, app_id  
    )
    select 
        total.date_key,
        total.app_id,
        coalesce(total.daily_fee_sum, 0) as daily_fee_sum,
        coalesce(total.daily_transaction_count, 0) as daily_total_transactions,
        coalesce(total.daily_amount, 0) as daily_total_amounts,
        coalesce(earns.daily_transaction_count, 0) as daily_earn_transactions,
        coalesce(earns.daily_amount, 0) as daily_earn_amounts,
        coalesce(earns.average_amount, 0) as daily_earn_average,
        coalesce(earns.median_amount, 0) as daily_earn_median,
        coalesce(spends.daily_transaction_count, 0) as daily_spend_transactions,
        coalesce(spends.daily_amount, 0) as daily_spend_amounts,
        coalesce(spends.average_amount, 0) as daily_spend_average,
        coalesce(spends.median_amount, 0) as daily_spend_median,
        coalesce(peer.daily_transaction_count, 0) as daily_peer_transactions,
        coalesce(peer.daily_amount, 0) as daily_peer_amounts,
        coalesce(peer.average_amount, 0) as daily_peer_average,
        coalesce(peer.median_amount, 0) as daily_peer_median,
        dka.name as app_name
    from daily_transactions_by_app  total
    left join daily_earns_by_app earns on total.date_key = earns.date_key
        and total.app_id = earns.app_id
    left join daily_spends_by_app spends on total.date_key = spends.date_key
        and total.app_id = spends.app_id
    left join daily_peer_by_app peer on total.date_key = peer.date_key
        and total.app_id = peer.app_id
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on total.app_id = dka.id
    order by date_key asc
{query_close}
