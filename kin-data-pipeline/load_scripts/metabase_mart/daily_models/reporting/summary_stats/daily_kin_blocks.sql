{create_or_insert}
    daily_blocks as (
        select 
            date_key,
            count(distinct block) as block_count
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key
    )
    select * from daily_blocks 
    order by date_key asc
{query_close}
