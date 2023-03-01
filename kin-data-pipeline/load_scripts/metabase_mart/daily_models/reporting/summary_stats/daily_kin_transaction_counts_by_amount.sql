{create_or_insert}
    transactions_by_amount as (
        select 
            date_key,
            case
                when amount::NUMERIC >= 1 and amount::NUMERIC < 2 then '1-2'
                when amount::NUMERIC >= 2 and amount::NUMERIC < 1000 then '2-1000'
                when amount::NUMERIC >= 1000 and amount::NUMERIC < 10000 then '1001-10000'
                when amount::NUMERIC >= 10000 and amount::NUMERIC < 100000 then '10001-100000'
                when amount::NUMERIC >= 100000 and amount::NUMERIC  < 1000000 then '100001-1000000'
                when amount::NUMERIC >= 1000000 then '1000001+'
                else 'N/A'
            end as amount_range
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
    ),
    daily_amounts_agg as (
        select
            date_key,
            amount_range,
            count(*) as transaction_count
        from transactions_by_amount
        group by date_key, amount_range
    )
    select * from daily_amounts_agg
    order by date_key asc
{query_close}
