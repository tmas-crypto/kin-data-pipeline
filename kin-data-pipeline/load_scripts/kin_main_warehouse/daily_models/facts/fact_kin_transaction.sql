{create_or_insert}
    -- TODO: Handle dupe checking during ingestion
    distinct_transactions as (
        select
            distinct txn.id as distinct_id,
            txn.date::DATE as date_key,
            txn.*
        from 
            shakudo_ingestion_db.kin_events.kintransaction4 txn
        where 
            date_key < date_trunc('day', current_date)
            {incremental_filter}  
    )
    /*
        Removed columns deemed un-necessary for analytic purposes
        - foreignkey
    */
    select
        distinct_id as id,
        date_trunc('day', date_key) as date_key,
        to_timestamp(timestamp) as timestamp,
        block,
        transactionid as transaction_id,
        instructionidx as instr_idx,
        -- 100000 is the factor applied to Kin values
        (coalesce(amount, 0)::NUMBER(38, 10)/100000) as amount,
        (coalesce(amount, 0)::NUMBER(38, 10)) as quark_amount,
        memo,
        fromaccount as from_account,
        fromaccountowner as from_account_owner,
        (coalesce(fromaccountbalance, 0)::NUMBER(38, 10)/100000) as from_account_balance,
        toaccount as to_account,
        toaccountowner as to_account_owner,
        (coalesce(toaccountbalance, 0)::NUMBER(38, 10)/100000) as to_account_balance,
        mint,
        -- 1000000000 is the factor applied to Sol values
        feepayer as fee_payer,
        (fee::NUMBER(38, 10)) as transaction_fee,
        case
            when appindex is null then 0
            else appindex
        end as app_id,
        -- Want to convert this to an int for dim mapping
        case
            when transactiontype = '<NA>' then null
            else transactiontype::NUMBER(38,0) 
        end as type
    from distinct_transactions
    -- We don't want any failed transactions in our fact table
    where status = 'Ok'
{query_close}
