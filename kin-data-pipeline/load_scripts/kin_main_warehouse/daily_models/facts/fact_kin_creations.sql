{create_or_insert}
    -- TODO: Handle dupe checking during ingestion
    distinct_transactions as (
        select
            distinct creates.id as distinct_id,
            creates.date::DATE as date_key,
            creates.*
        from 
            shakudo_ingestion_db.kin_events.kincreateaccountnew creates
        where
            status = 'Ok' and 
            date_key < date_trunc('day', current_date)
            {incremental_filter}  
    )
    /*
        Removed columns deemed un-necessary for analytic purposes
        - status
        - memoobj
        - foreignkey
    */
    select
        distinct_id as id,
        date_trunc('day', date_key) as date_key,
        to_timestamp(timestamp) as timestamp,
        program_id,
        block,
        tx_id as transaction_id,
        instr_idx,
        memo,
        account_id,
        account_owner,
        close_account_authority,
        mint,
        source_transfer,
        -- 100000 is the factor applied to Kin values
        (coalesce(balance, 0)::NUMBER(38, 10)/100000) as balance,
        -- 1000000000 is the factor applied to Sol values
        (coalesce(fee, 0)::NUMBER(38, 10)/1000000000) as transaction_fee,
        (create_fee::NUMBER(38, 10)/1000000000) as creation_fee,
        fee_payer,
        case
            when appindex is null then 0
            else appindex
        end as app_id,
        -- Want to convert this to an int for dim mapping
        case 
            when transactiontype = '<NA>' then null
            else transactiontype::NUMBER(38,0) 
        end as type,
        version::NUMBER(38,0) as version
    from distinct_transactions
    -- We only want successful creates ingested into our fact table
    where status = 'Ok'
{query_close}
