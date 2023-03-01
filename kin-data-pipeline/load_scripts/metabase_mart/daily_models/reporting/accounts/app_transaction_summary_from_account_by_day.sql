{create_or_insert}
    transaction_summary_from_account_by_day as (
        select 
            date_key,
            ifnull(app_id, 0) as app_id,
            from_account as wallet_id,
            count(id) as transaction_count,
            sum(amount) as amount
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
        group by date_key, app_id, wallet_id
    )
    select tsfabd.*, dka.name as app_name
    from transaction_summary_from_account_by_day tsfabd
    left join {target_database}.kin_main_warehouse.dim_kin_app dka on tsfabd.app_id = dka.id
    order by date_key asc
{query_close}
