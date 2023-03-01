{create_or_insert}
    daily_creates_by_app_and_fees as (
        select 
            date_key,
            app_id,
            fee_payer,
            count(id) as daily_transaction_count,
            sum(transaction_fee) as daily_transaction_fee_sum,
            sum(creation_fee) as daily_creation_fee_sum
        from {target_database}.kin_main_warehouse.fact_kin_creations
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, fee_payer 
    )
    select 
        fees.date_key,
        fees.app_id,
        fees.fee_payer,
        fees.daily_transaction_count as daily_transactions,
        fees.daily_transaction_fee_sum as daily_transaction_fees_sol,
        (fees.daily_transaction_fee_sum*sol_price.prices) as daily_transaction_fees_usd,
        fees.daily_creation_fee_sum as daily_creation_fees_sol,
        (fees.daily_creation_fee_sum*sol_price.prices) as daily_creation_fees_usd,
        dka.name as app_name
    from daily_creates_by_app_and_fees fees
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on fees.app_id = dka.id
    left join {target_database}.kin_main_warehouse.dim_solana_price sol_price on fees.date_key = sol_price.date_key
    order by date_key asc
{query_close}
