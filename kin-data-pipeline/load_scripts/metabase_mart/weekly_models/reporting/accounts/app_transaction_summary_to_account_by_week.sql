{create_or_insert}
    transaction_summary_to_account_by_week as (
        select 
            date_trunc('week', date_key) as date_key,
            app_id,
            wallet_id,
            sum(transaction_count) as transaction_count,
            sum(amount) as amount
        from {target_database}.metabase_mart.app_transaction_summary_to_account_by_day
        where date_key < date_trunc('week', current_date)
        {incremental_filter}
        group by date_trunc('week', date_key), app_id, wallet_id
    )
    select tstabw.*, dka.name as app_name
    from transaction_summary_to_account_by_week tstabw
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on tstabw.app_id = dka.id
    order by date_key asc
{query_close}
