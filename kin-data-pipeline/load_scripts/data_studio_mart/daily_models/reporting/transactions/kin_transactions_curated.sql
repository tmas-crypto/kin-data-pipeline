{create_or_insert}
    daily_transactions as (
        select 
            id as transaction_id,
            transaction_id as transaction_hash,
            date_key,
            app_id,
            tx_status as transaction_status,
            type,
            amount,
            quark_amount as quarks,
            block,
            instr_idx as instruction_offset,
            memo as memo_text,
            timestamp as date_time,
            from_account as source,
            from_account_owner as source_owner,
            from_account_balance as source_balance,
            to_account as destination,
            to_account_owner as destination_owner,
            to_account_balance as destination_balance,
            fee_payer as subsidizer,
            transaction_fee as fee
        from {target_database}.kin_main_warehouse.fact_kin_transaction
        -- We only want complete days
        where date_key < date_trunc('day', current_date)
        {incremental_filter}
    )
    select *
    from daily_transactions
    -- Filter out non app associated transactions
    where app_id != 0
    order by date_key desc
{query_close}
