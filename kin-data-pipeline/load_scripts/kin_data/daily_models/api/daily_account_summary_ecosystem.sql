{create_or_insert}
    creates as (
        select
            date_key as pg_date_key,
            sum(daily_transactions) as accounts_created,
            sum(daily_transaction_fee_sum) as transction_fees,
            sum(daily_creation_fee_sum) as creation_fees
        from {target_database}.metabase_mart.daily_account_creates_by_app
        where True
        {incremental_filter}
        group by date_key
    ),
    sol_prices as (
        select
            date_key as pg_date_key,
            prices as daily_price
        from {target_database}.kin_main_warehouse.dim_solana_price
        where True
        {incremental_filter}
    )
    select 
        c.pg_date_key as date_key,
        c.accounts_created as accounts_created,
        -- Creates costs = creation fee + transaction fee
        (transction_fees + creation_fees) as sol_cost,
        ((transction_fees + creation_fees) * s.daily_price) as usd_cost
    from creates c
    join sol_prices s on c.pg_date_key = s.pg_date_key
    order by c.pg_date_key asc
{query_close}
