{create_or_insert}
    daily_creates_by_app_and_program as (
        select 
            date_key,
            app_id,
            program_id as program_id,
            count(id) as daily_transaction_count,
            sum(balance) as daily_opening_balance
        from {target_database}.kin_main_warehouse.fact_kin_creations
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, program_id 
    )
    select 
        programs.date_key,
        programs.app_id,
        programs.program_id,
        programs.daily_transaction_count as daily_transactions,
        programs.daily_opening_balance as daily_opening_balance,
        dka.name as app_name
    from daily_creates_by_app_and_program programs
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on programs.app_id = dka.id
    order by date_key asc
{query_close}
